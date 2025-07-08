#!/usr/bin/env python3
"""
Fund ETL API Service

Provides RESTful API endpoints for triggering ETL operations.
Runs inside the fund-etl container and can be called by the UI container.
"""

from flask import Flask, jsonify, request
import subprocess
import threading
import uuid
import json
import os
from datetime import datetime
import logging
import sys

# Import the database-backed workflow tracker
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from workflow_db_tracker import DatabaseWorkflowTracker

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/logs/fund_etl_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database-backed workflow tracking
DB_PATH = os.environ.get('DB_PATH', '/data/fund_data.db')
workflow_tracker = DatabaseWorkflowTracker(DB_PATH)

# Keep in-memory workflows for backward compatibility during transition
workflows = {}
workflow_lock = threading.Lock()

# Global ETL execution lock to prevent concurrent ETL processes
etl_execution_lock = threading.Lock()
running_etl_workflows = set()  # Track currently running ETL workflows

def is_etl_running():
    """Check if any ETL process is currently running"""
    with etl_execution_lock:
        return len(running_etl_workflows) > 0

def can_start_etl(workflow_type, workflow_id):
    """Check if we can start a new ETL process and reserve the slot atomically"""
    with etl_execution_lock:
        # Don't allow any concurrent ETL operations
        if len(running_etl_workflows) == 0:
            running_etl_workflows.add(workflow_id)
            return True
        return False

def run_etl_process(workflow_id, command_args):
    """Run ETL process in background thread"""
    try:
        # Workflow ID should already be in running_etl_workflows from can_start_etl()
        
        # Update both in-memory and database
        with workflow_lock:
            workflows[workflow_id]['status'] = 'running'
            workflows[workflow_id]['started_at'] = datetime.now().isoformat()
        
        workflow_tracker.update_workflow(workflow_id, status='running')
        
        logger.info(f"Starting workflow {workflow_id} with command: {' '.join(command_args)}")
        
        # Run the command
        process = subprocess.Popen(
            command_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        output_lines = []
        
        # Capture output
        for line in iter(process.stdout.readline, ''):
            if line:
                output_lines.append({
                    'timestamp': datetime.now().isoformat(),
                    'message': line.strip()
                })
                # Update workflow output
                with workflow_lock:
                    workflows[workflow_id]['output'] = output_lines[-100:]  # Keep last 100 lines
                
                # Also update in database
                workflow_tracker.update_workflow(workflow_id, output_line=line.strip())
        
        process.wait()
        
        # Update workflow status
        with workflow_lock:
            workflows[workflow_id]['completed_at'] = datetime.now().isoformat()
            if process.returncode == 0:
                workflows[workflow_id]['status'] = 'completed'
                workflows[workflow_id]['message'] = 'Workflow completed successfully'
            else:
                workflows[workflow_id]['status'] = 'failed'
                workflows[workflow_id]['error'] = f'Process exited with code {process.returncode}'
        
        # Update database
        if process.returncode == 0:
            workflow_tracker.update_workflow(workflow_id, status='completed', message='Workflow completed successfully')
        else:
            workflow_tracker.update_workflow(workflow_id, status='failed', error=f'Process exited with code {process.returncode}')
        
        logger.info(f"Workflow {workflow_id} completed with status: {workflows[workflow_id]['status']}")
        
    except Exception as e:
        logger.error(f"Error in workflow {workflow_id}: {str(e)}")
        with workflow_lock:
            workflows[workflow_id]['status'] = 'failed'
            workflows[workflow_id]['error'] = str(e)
            workflows[workflow_id]['completed_at'] = datetime.now().isoformat()
        
        # Update database
        workflow_tracker.update_workflow(workflow_id, status='failed', error=str(e))
    finally:
        # Remove from running workflows
        with etl_execution_lock:
            running_etl_workflows.discard(workflow_id)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'fund-etl-api',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/etl/run-daily', methods=['POST'])
def run_daily_etl():
    """Trigger daily ETL run"""
    try:
        workflow_id = str(uuid.uuid4())
        
        # Check if ETL is already running and reserve slot atomically
        if not can_start_etl('daily-etl', workflow_id):
            return jsonify({
                'error': 'An ETL process is already running. Please wait for it to complete.',
                'status': 'rejected'
            }), 409
        
        # Initialize workflow in both memory and database
        with workflow_lock:
            workflows[workflow_id] = {
                'id': workflow_id,
                'type': 'daily-etl',
                'status': 'pending',
                'created_at': datetime.now().isoformat(),
                'started_at': None,
                'completed_at': None,
                'output': [],
                'error': None
            }
        
        # Also create in database with same workflow_id
        workflow_tracker.start_workflow('daily-etl', {}, workflow_id=workflow_id)
        
        # Run in background thread
        thread = threading.Thread(
            target=run_etl_process,
            args=(workflow_id, ['python', '/app/fund_etl_scheduler.py', '--run-daily'])
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'workflow_id': workflow_id,
            'status': 'started',
            'message': 'Daily ETL workflow started'
        }), 202
        
    except Exception as e:
        logger.error(f"Error starting daily ETL: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/etl/validate', methods=['POST'])
def run_validation():
    """Trigger validation run"""
    try:
        # Get validation mode from request
        data = request.get_json() or {}
        mode = data.get('mode', 'selective')  # selective or full
        
        if mode not in ['selective', 'full']:
            return jsonify({'error': 'Invalid mode. Must be "selective" or "full"'}), 400
        
        workflow_id = str(uuid.uuid4())
        
        # Check if ETL is already running and reserve slot atomically
        if not can_start_etl(f'validation-{mode}', workflow_id):
            return jsonify({
                'error': 'An ETL process is already running. Please wait for it to complete.',
                'status': 'rejected'
            }), 409
        
        # Initialize workflow in both memory and database
        with workflow_lock:
            workflows[workflow_id] = {
                'id': workflow_id,
                'type': f'validation-{mode}',
                'status': 'pending',
                'created_at': datetime.now().isoformat(),
                'started_at': None,
                'completed_at': None,
                'output': [],
                'error': None,
                'params': {'mode': mode}
            }
        
        # Also create in database with same workflow_id
        workflow_tracker.start_workflow(f'validation-{mode}', {'mode': mode}, workflow_id=workflow_id)
        
        # Determine command based on mode
        if mode == 'full':
            cmd_args = ['python', '/app/fund_etl_scheduler.py', '--validate-full']
        else:
            cmd_args = ['python', '/app/fund_etl_scheduler.py', '--validate']
        
        # Run in background thread
        thread = threading.Thread(
            target=run_etl_process,
            args=(workflow_id, cmd_args)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'workflow_id': workflow_id,
            'status': 'started',
            'message': f'Validation workflow started in {mode} mode'
        }), 202
        
    except Exception as e:
        logger.error(f"Error starting validation: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/etl/run-date', methods=['POST'])
def run_etl_for_date():
    """Run ETL for a specific date"""
    try:
        data = request.get_json() or {}
        target_date = data.get('date')
        
        if not target_date:
            return jsonify({'error': 'Date parameter is required'}), 400
        
        # Validate date format
        try:
            datetime.strptime(target_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        workflow_id = str(uuid.uuid4())
        
        # Check if ETL is already running and reserve slot atomically
        if not can_start_etl('run-date', workflow_id):
            return jsonify({
                'error': 'An ETL process is already running. Please wait for it to complete.',
                'status': 'rejected'
            }), 409
        
        # Initialize workflow in both memory and database
        with workflow_lock:
            workflows[workflow_id] = {
                'id': workflow_id,
                'type': 'run-date',
                'status': 'pending',
                'created_at': datetime.now().isoformat(),
                'started_at': None,
                'completed_at': None,
                'output': [],
                'error': None,
                'params': {'date': target_date}
            }
        
        # Also create in database with same workflow_id
        workflow_tracker.start_workflow('run-date', {'date': target_date}, workflow_id=workflow_id)
        
        # Run in background thread
        thread = threading.Thread(
            target=run_etl_process,
            args=(workflow_id, ['python', '/app/fund_etl_scheduler.py', '--run-date', target_date])
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'workflow_id': workflow_id,
            'status': 'started',
            'message': f'ETL workflow started for date {target_date}'
        }), 202
        
    except Exception as e:
        logger.error(f"Error starting ETL for date: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/etl/workflow/<workflow_id>', methods=['GET'])
def get_workflow_status(workflow_id):
    """Get status of a specific workflow"""
    # Try database first
    workflow = workflow_tracker.get_workflow(workflow_id)
    
    if not workflow:
        # Fallback to in-memory for backward compatibility
        with workflow_lock:
            workflow = workflows.get(workflow_id)
    
    if not workflow:
        return jsonify({'error': 'Workflow not found'}), 404
    
    return jsonify(workflow)

@app.route('/api/etl/workflows', methods=['GET'])
def list_workflows():
    """List all workflows"""
    # Get workflows from database
    all_workflows = workflow_tracker.get_all_workflows(limit=100)
    
    # Optionally filter by status
    status_filter = request.args.get('status')
    if status_filter:
        all_workflows = [w for w in all_workflows if w['status'] == status_filter]
    
    # Limit to last 50 workflows
    return jsonify(all_workflows[:50])

@app.route('/api/etl/cleanup', methods=['POST'])
def cleanup_workflows():
    """Clean up old completed workflows"""
    try:
        data = request.get_json() or {}
        hours = data.get('hours', 24)
        
        # Clean up database workflows
        db_removed = workflow_tracker.cleanup_old_workflows(hours)
        
        # Also clean up in-memory workflows
        cutoff = datetime.now().timestamp() - (hours * 3600)
        memory_removed = 0
        with workflow_lock:
            to_remove = []
            for wf_id, wf in workflows.items():
                if wf.get('completed_at'):
                    completed_time = datetime.fromisoformat(wf['completed_at']).timestamp()
                    if completed_time < cutoff:
                        to_remove.append(wf_id)
            
            for wf_id in to_remove:
                del workflows[wf_id]
                memory_removed += 1
        
        return jsonify({
            'message': f'Removed {db_removed} database workflows and {memory_removed} memory workflows older than {hours} hours'
        })
        
    except Exception as e:
        logger.error(f"Error cleaning up workflows: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Run the API server
    app.run(host='0.0.0.0', port=8081, debug=False)