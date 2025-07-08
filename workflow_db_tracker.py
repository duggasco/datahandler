#!/usr/bin/env python3
"""
Database-backed workflow tracker for persistent workflow management
"""

import sqlite3
import json
import uuid
import threading
from datetime import datetime
from typing import Optional, Dict, List


class DatabaseWorkflowTracker:
    """Track workflows persistently in SQLite database"""
    
    def __init__(self, db_path: str = '/data/fund_data.db'):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._ensure_table_exists()
    
    def _ensure_table_exists(self):
        """Ensure the workflows table exists"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS workflows (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                params TEXT,
                output TEXT,
                error TEXT,
                message TEXT,
                etl_workflow_id TEXT
            )
            """)
            
            # Create indices if they don't exist
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_workflows_status 
            ON workflows(status)
            """)
            
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_workflows_created_at 
            ON workflows(created_at DESC)
            """)
            
            conn.commit()
    
    def start_workflow(self, workflow_type: str, params: Optional[Dict] = None, workflow_id: Optional[str] = None) -> str:
        """Start tracking a new workflow"""
        if workflow_id is None:
            workflow_id = str(uuid.uuid4())
        
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                INSERT INTO workflows (
                    id, type, status, created_at, started_at, params, output
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    workflow_id,
                    workflow_type,
                    'pending',  # Start as pending, not running
                    datetime.now().isoformat(),
                    None,  # Not started yet
                    json.dumps(params or {}),
                    json.dumps([])
                ))
                conn.commit()
        
        return workflow_id
    
    def update_workflow(self, workflow_id: str, output_line: Optional[str] = None, 
                       status: Optional[str] = None, error: Optional[str] = None,
                       message: Optional[str] = None, etl_workflow_id: Optional[str] = None):
        """Update workflow status or add output"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get current workflow data
                cursor.execute("""
                SELECT output, status FROM workflows WHERE id = ?
                """, (workflow_id,))
                
                row = cursor.fetchone()
                if not row:
                    return
                
                current_output = json.loads(row[0] or '[]')
                current_status = row[1]
                
                # Add output line if provided
                if output_line:
                    current_output.append({
                        'timestamp': datetime.now().isoformat(),
                        'message': output_line
                    })
                    # Keep only last 100 messages
                    current_output = current_output[-100:]
                
                # Build update query
                updates = ['output = ?']
                params = [json.dumps(current_output)]
                
                if status:
                    updates.append('status = ?')
                    params.append(status)
                    
                    if status in ['completed', 'failed']:
                        updates.append('completed_at = ?')
                        params.append(datetime.now().isoformat())
                
                if error:
                    updates.append('error = ?')
                    params.append(error)
                
                if message:
                    updates.append('message = ?')
                    params.append(message)
                
                if etl_workflow_id:
                    updates.append('etl_workflow_id = ?')
                    params.append(etl_workflow_id)
                
                # Execute update
                params.append(workflow_id)
                cursor.execute(f"""
                UPDATE workflows 
                SET {', '.join(updates)}
                WHERE id = ?
                """, params)
                
                conn.commit()
    
    def get_workflow(self, workflow_id: str) -> Optional[Dict]:
        """Get workflow status"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                SELECT id, type, status, created_at, started_at, completed_at,
                       params, output, error, message, etl_workflow_id
                FROM workflows
                WHERE id = ?
                """, (workflow_id,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                return {
                    'id': row[0],
                    'type': row[1],
                    'status': row[2],
                    'created_at': row[3],
                    'started_at': row[4],
                    'completed_at': row[5],
                    'params': json.loads(row[6] or '{}'),
                    'output': json.loads(row[7] or '[]'),
                    'error': row[8],
                    'message': row[9],
                    'etl_workflow_id': row[10]
                }
    
    def get_all_workflows(self, limit: int = 50) -> List[Dict]:
        """Get all workflows, sorted by created_at descending"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                SELECT id, type, status, created_at, started_at, completed_at,
                       params, output, error, message, etl_workflow_id
                FROM workflows
                ORDER BY created_at DESC
                LIMIT ?
                """, (limit,))
                
                workflows = []
                for row in cursor.fetchall():
                    workflows.append({
                        'id': row[0],
                        'type': row[1],
                        'status': row[2],
                        'created_at': row[3],
                        'started_at': row[4],
                        'completed_at': row[5],
                        'params': json.loads(row[6] or '{}'),
                        'output': json.loads(row[7] or '[]'),
                        'error': row[8],
                        'message': row[9],
                        'etl_workflow_id': row[10]
                    })
                
                return workflows
    
    def cleanup_old_workflows(self, hours: int = 24):
        """Remove workflows older than specified hours"""
        cutoff = datetime.now().timestamp() - (hours * 3600)
        cutoff_iso = datetime.fromtimestamp(cutoff).isoformat()
        
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                DELETE FROM workflows
                WHERE completed_at IS NOT NULL 
                AND completed_at < ?
                """, (cutoff_iso,))
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                return deleted_count
    
    def sync_with_backend_workflows(self, backend_workflows: List[Dict]):
        """Sync workflows from backend API (for migration/recovery)"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for wf in backend_workflows:
                    # Check if workflow already exists
                    cursor.execute("SELECT id FROM workflows WHERE id = ?", (wf['id'],))
                    
                    if not cursor.fetchone():
                        # Insert new workflow
                        cursor.execute("""
                        INSERT INTO workflows (
                            id, type, status, created_at, started_at, completed_at,
                            params, output, error, message
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            wf['id'],
                            wf.get('type', 'unknown'),
                            wf.get('status', 'unknown'),
                            wf.get('created_at', datetime.now().isoformat()),
                            wf.get('started_at'),
                            wf.get('completed_at'),
                            json.dumps(wf.get('params', {})),
                            json.dumps(wf.get('output', [])),
                            wf.get('error'),
                            wf.get('message')
                        ))
                
                conn.commit()