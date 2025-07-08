#!/usr/bin/env python3
"""
Workflow and API Tests
Tests workflow management and API endpoints
"""

import unittest
import json
import time
import threading
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import requests
from flask import Flask
from pathlib import Path

from test_framework import ETLTestCase, APITestMixin
from workflow_db_tracker import DatabaseWorkflowTracker


class TestWorkflowTracking(ETLTestCase):
    """Test workflow tracking functionality"""
    
    def setUp(self):
        super().setUp()
        self.tracker = DatabaseWorkflowTracker(str(self.test_db))
    
    def test_workflow_lifecycle(self):
        """Test complete workflow lifecycle"""
        # Create workflow
        workflow_id = self.tracker.start_workflow(
            'test-etl', 
            {'date': '2024-01-15', 'region': 'AMRS'}
        )
        
        # Verify initial state
        workflow = self.tracker.get_workflow(workflow_id)
        self.assertEqual(workflow['status'], 'pending')
        self.assertEqual(workflow['type'], 'test-etl')
        self.assertIsNone(workflow['started_at'])
        
        # Start workflow
        self.tracker.update_workflow(workflow_id, status='running')
        workflow = self.tracker.get_workflow(workflow_id)
        self.assertEqual(workflow['status'], 'running')
        self.assertIsNotNone(workflow['started_at'])
        
        # Add output
        self.tracker.update_workflow(
            workflow_id, 
            output_line='Starting ETL process'
        )
        self.tracker.update_workflow(
            workflow_id, 
            output_line='Processing AMRS data'
        )
        
        workflow = self.tracker.get_workflow(workflow_id)
        self.assertEqual(len(workflow['output']), 2)
        
        # Complete workflow
        self.tracker.update_workflow(
            workflow_id, 
            status='completed',
            message='ETL completed successfully'
        )
        
        workflow = self.tracker.get_workflow(workflow_id)
        self.assertEqual(workflow['status'], 'completed')
        self.assertIsNotNone(workflow['completed_at'])
        self.assertEqual(workflow['message'], 'ETL completed successfully')
    
    def test_workflow_error_handling(self):
        """Test workflow error states"""
        workflow_id = self.tracker.start_workflow('error-test')
        
        # Simulate error
        self.tracker.update_workflow(workflow_id, status='running')
        self.tracker.update_workflow(
            workflow_id,
            status='failed',
            error='Connection timeout'
        )
        
        workflow = self.tracker.get_workflow(workflow_id)
        self.assertEqual(workflow['status'], 'failed')
        self.assertEqual(workflow['error'], 'Connection timeout')
        self.assertIsNotNone(workflow['completed_at'])
    
    def test_workflow_output_limit(self):
        """Test workflow output is limited to prevent memory issues"""
        workflow_id = self.tracker.start_workflow('output-test')
        
        # Add many output lines
        for i in range(150):
            self.tracker.update_workflow(
                workflow_id,
                output_line=f'Output line {i}'
            )
        
        workflow = self.tracker.get_workflow(workflow_id)
        
        # Should only keep last 100 lines
        self.assertEqual(len(workflow['output']), 100)
        self.assertEqual(workflow['output'][0]['message'], 'Output line 50')
        self.assertEqual(workflow['output'][-1]['message'], 'Output line 149')
    
    def test_workflow_etl_linkage(self):
        """Test linking frontend and backend workflows"""
        # Create frontend workflow
        ui_workflow_id = self.tracker.start_workflow('validation')
        
        # Link to backend ETL workflow
        etl_workflow_id = 'etl-123-456'
        self.tracker.update_workflow(
            ui_workflow_id,
            etl_workflow_id=etl_workflow_id
        )
        
        workflow = self.tracker.get_workflow(ui_workflow_id)
        self.assertEqual(workflow['etl_workflow_id'], etl_workflow_id)
    
    def test_workflow_listing(self):
        """Test listing workflows"""
        # Create multiple workflows
        ids = []
        for i in range(5):
            wf_id = self.tracker.start_workflow(
                f'list-test-{i}',
                {'index': i}
            )
            ids.append(wf_id)
            time.sleep(0.01)  # Ensure different timestamps
        
        # Get all workflows
        workflows = self.tracker.get_all_workflows()
        
        self.assertGreaterEqual(len(workflows), 5)
        
        # Should be sorted by created_at descending
        for i in range(1, len(workflows)):
            self.assertGreaterEqual(
                workflows[i-1]['created_at'],
                workflows[i]['created_at']
            )
    
    def test_concurrent_workflow_updates(self):
        """Test thread-safe concurrent updates"""
        workflow_id = self.tracker.start_workflow('concurrent-test')
        
        def update_worker(worker_id):
            for i in range(10):
                self.tracker.update_workflow(
                    workflow_id,
                    output_line=f'Worker {worker_id} - Line {i}'
                )
        
        # Start multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=update_worker, args=(i,))
            threads.append(t)
            t.start()
        
        # Wait for completion
        for t in threads:
            t.join()
        
        # Should have all updates
        workflow = self.tracker.get_workflow(workflow_id)
        self.assertEqual(len(workflow['output']), 50)


class TestETLAPI(ETLTestCase):
    """Test ETL API endpoints"""
    

    def tearDown(self):
        """Clean up after each test"""
        # Clear any running workflows
        try:
            import fund_etl_api
            fund_etl_api.running_etl_workflows.clear()
            fund_etl_api.workflows.clear()
            
            # Clean up all test workflows from database
            if hasattr(self, 'test_db'):
                conn = sqlite3.connect(str(self.test_db))
                cursor = conn.cursor()
                try:
                    cursor.execute("DELETE FROM workflows")
                    conn.commit()
                except sqlite3.OperationalError:
                    # Table may not exist in some tests
                    pass
                conn.close()
        except:
            pass
        super().tearDown()

    def setUp(self):
        super().setUp()
        # Import here to avoid circular imports
        from fund_etl_api import app as api_app
        self.app = api_app.test_client()
        self.app.testing = True
        
        # Clean up any running workflows
        import fund_etl_api
        fund_etl_api.running_etl_workflows.clear()
        fund_etl_api.workflows.clear()
        
        # Reset workflow tracker to use test database
        fund_etl_api.workflow_tracker.reset(str(self.test_db))
    
    def test_health_endpoint(self):
        """Test health check endpoint"""
        response = self.app.get('/health')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['status'], 'healthy')
        self.assertEqual(data['service'], 'fund-etl-api')
    
    @patch('fund_etl_api.subprocess.Popen')
    def test_run_daily_etl(self, mock_popen):
        """Test daily ETL trigger"""
        # Mock subprocess
        mock_process = Mock()
        mock_process.stdout.readline.side_effect = [
            'Starting ETL\n',
            'Processing data\n',
            ''  # End of output
        ]
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        mock_popen.return_value = mock_process
        
        # Trigger ETL
        response = self.app.post('/api/etl/run-daily')
        
        self.assertEqual(response.status_code, 202)
        data = json.loads(response.data)
        self.assertIn('workflow_id', data)
        self.assertEqual(data['status'], 'started')
    
    @patch('fund_etl_api.subprocess.Popen')
    def test_validation_endpoints(self, mock_popen):
        """Test validation trigger endpoints"""
        # Clear any running workflows first
        import fund_etl_api
        fund_etl_api.running_etl_workflows.clear()
        
        # Mock subprocess
        mock_process = Mock()
        mock_process.stdout.readline.side_effect = ['', '']
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        mock_popen.return_value = mock_process
        
        # Test selective validation
        response = self.app.post(
            '/api/etl/validate',
            json={'mode': 'selective'}
        )
        
        self.assertEqual(response.status_code, 202)
        data = json.loads(response.data)
        self.assertIn('workflow_id', data)
        self.assertIn('selective', data['message'])
        
        # Clear running workflows before second test
        fund_etl_api.running_etl_workflows.clear()
        
        # Test full validation
        response = self.app.post(
            '/api/etl/validate',
            json={'mode': 'full'}
        )
        
        self.assertEqual(response.status_code, 202)
    
    def test_invalid_validation_mode(self):
        """Test invalid validation mode rejection"""
        response = self.app.post(
            '/api/etl/validate',
            json={'mode': 'invalid'}
        )
        
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertIn('error', data)
    
    @patch('fund_etl_api.subprocess.Popen')
    def test_run_specific_date(self, mock_popen):
        """Test running ETL for specific date"""
        # Mock subprocess
        mock_process = Mock()
        mock_process.stdout.readline.side_effect = ['', '']
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        mock_popen.return_value = mock_process
        
        response = self.app.post(
            '/api/etl/run-date',
            json={'date': '2024-01-15'}
        )
        
        self.assertEqual(response.status_code, 202)
        data = json.loads(response.data)
        self.assertIn('2024-01-15', data['message'])
    
    def test_invalid_date_format(self):
        """Test invalid date format rejection"""
        response = self.app.post(
            '/api/etl/run-date',
            json={'date': '01/15/2024'}  # Wrong format
        )
        
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertIn('Invalid date format', data['error'])
    
    def test_concurrent_etl_prevention(self):
        """Test prevention of concurrent ETL runs"""
        # First start an ETL run
        with patch('fund_etl_api.subprocess.Popen') as mock_popen:
            mock_process = Mock()
            mock_process.stdout.readline.side_effect = ['Starting', '']
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process
            
            # Start first ETL
            self.app.post('/api/etl/run-daily')
        
        # Now try to start another
        response = self.app.post('/api/etl/run-daily')
        
        # Should get 409 conflict
        self.assertEqual(response.status_code, 409)
        data = json.loads(response.data)
        self.assertIn('already running', data['error'])
    
    def test_workflow_status_endpoint(self):
        """Test workflow status retrieval"""
        # Create a workflow directly
        with patch('fund_etl_api.workflow_tracker') as mock_tracker:
            mock_tracker.get_workflow.return_value = {
                'id': 'test-123',
                'type': 'daily-etl',
                'status': 'running',
                'output': []
            }
            
            response = self.app.get('/api/etl/workflow/test-123')
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data)
            self.assertEqual(data['id'], 'test-123')
            self.assertEqual(data['status'], 'running')
    
    def test_workflow_not_found(self):
        """Test workflow not found response"""
        with patch('fund_etl_api.workflow_tracker') as mock_tracker:
            mock_tracker.get_workflow.return_value = None
            
            response = self.app.get('/api/etl/workflow/non-existent')
            
            self.assertEqual(response.status_code, 404)
    
    def test_list_workflows(self):
        """Test listing workflows"""
        with patch('fund_etl_api.workflow_tracker') as mock_tracker:
            mock_tracker.get_all_workflows.return_value = [
                {'id': '1', 'type': 'daily-etl', 'status': 'completed'},
                {'id': '2', 'type': 'validation', 'status': 'running'}
            ]
            
            response = self.app.get('/api/etl/workflows')
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data)
            self.assertEqual(len(data), 2)
    
    def test_cleanup_workflows(self):
        """Test workflow cleanup endpoint"""
        with patch('fund_etl_api.workflow_tracker') as mock_tracker:
            mock_tracker.cleanup_old_workflows.return_value = 5
            
            response = self.app.post(
                '/api/etl/cleanup',
                json={'hours': 48}
            )
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data)
            self.assertIn('5', data['message'])


class TestUIAPI(ETLTestCase, APITestMixin):
    """Test UI API endpoints"""
    
    def tearDown(self):
        """Clean up after each test"""
        # Clean up all test workflows from database
        try:
            if hasattr(self, 'test_db'):
                conn = sqlite3.connect(str(self.test_db))
                cursor = conn.cursor()
                try:
                    cursor.execute("DELETE FROM workflows")
                    conn.commit()
                except sqlite3.OperationalError:
                    # Table may not exist in some tests
                    pass
                conn.close()
        except:
            pass
        super().tearDown()
    
    def setUp(self):
        super().setUp()
        # Import here to avoid circular imports
        from fund_etl_ui import app as ui_app
        self.app = ui_app.test_client()
        self.app.testing = True
        
        # Create test database
        self.create_test_database()
        
        # Reset workflow tracker to use test database
        import fund_etl_ui
        fund_etl_ui.workflow_tracker.reset(str(self.test_db))
    
    def test_health_endpoint(self):
        """Test UI health check"""
        with patch('fund_etl_ui.os.path.exists', return_value=True):
            with patch('builtins.open', unittest.mock.mock_open(
                read_data='{"status": "healthy"}'
            )):
                response = self.app.get('/api/health')
                
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data)
                self.assertEqual(data['status'], 'healthy')
    
    def test_fund_data_endpoint(self):
        """Test fund data retrieval"""
        # Insert test data
        conn = self.create_test_database()
        self.insert_test_data(conn, num_records=5)
        conn.close()
        
        with patch('fund_etl_ui.DB_PATH', str(self.test_db)):
            response = self.app.get('/api/fund-data')
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data)
            self.assertEqual(len(data), 5)
            self.assertEqual(data[0]['fund_code'], 'TEST0000')
    
    def test_etl_log_endpoint(self):
        """Test ETL log retrieval"""
        # Insert test log entry
        conn = self.create_test_database()
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO etl_log (
            run_date, region, file_date, status, records_processed
        ) VALUES (?, ?, ?, ?, ?)
        """, ('2024-01-15', 'AMRS', '2024-01-15', 'SUCCESS', 1500))
        conn.commit()
        conn.close()
        
        with patch('fund_etl_ui.DB_PATH', str(self.test_db)):
            response = self.app.get('/api/etl-log')
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data)
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0]['status'], 'SUCCESS')
    
    def test_telemetry_endpoint(self):
        """Test telemetry data"""
        with patch('fund_etl_ui.DB_PATH', str(self.test_db)):
            with patch('fund_etl_ui.os.path.getsize', return_value=10485760):
                response = self.app.get('/api/telemetry')
                
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data)
                self.assertIn('db_size', data)
                self.assertIn('table_count', data)
                self.assertIn('unique_funds', data)
    
    @patch('requests.post')
    def test_run_daily_workflow_ui(self, mock_post):
        """Test UI workflow trigger"""
        # Mock ETL API response
        mock_response = Mock()
        mock_response.status_code = 202
        mock_response.json.return_value = {
            'workflow_id': 'etl-123',
            'status': 'started'
        }
        mock_post.return_value = mock_response
        
        with patch('fund_etl_ui.workflow_tracker') as mock_tracker:
            mock_tracker.start_workflow.return_value = 'ui-456'
            
            response = self.app.post('/api/workflow/run-daily')
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data)
            self.assertEqual(data['workflow_id'], 'ui-456')
            self.assertEqual(data['etl_workflow_id'], 'etl-123')
    
    @patch('requests.post')
    def test_validation_workflow_ui(self, mock_post):
        """Test UI validation trigger"""
        # Mock ETL API response
        mock_response = Mock()
        mock_response.status_code = 202
        mock_response.json.return_value = {
            'workflow_id': 'etl-val-123',
            'status': 'started'
        }
        mock_post.return_value = mock_response
        
        with patch('fund_etl_ui.workflow_tracker') as mock_tracker:
            mock_tracker.start_workflow.return_value = 'ui-val-456'
            
            response = self.app.post(
                '/api/workflow/validate',
                json={'mode': 'selective'}
            )
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data)
            self.assertIn('selective', data['message'])
    
    def test_workflow_list_ui(self):
        """Test UI workflow list"""
        with patch('fund_etl_ui.workflow_tracker') as mock_tracker:
            mock_tracker.get_all_workflows.return_value = [
                {
                    'id': '1',
                    'type': 'validation',
                    'status': 'running',
                    'started_at': datetime.now().isoformat()
                },
                {
                    'id': '2',
                    'type': 'daily-etl',
                    'status': 'completed',
                    'started_at': datetime.now().isoformat()
                }
            ]
            
            response = self.app.get('/api/workflow/list')
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data)
            self.assertEqual(len(data), 2)
    
    def test_export_fund_data(self):
        """Test fund data export"""
        # Insert test data
        conn = self.create_test_database()
        self.insert_test_data(conn, num_records=3)
        conn.close()
        
        with patch('fund_etl_ui.DB_PATH', str(self.test_db)):
            response = self.app.get('/api/export/fund-data')
            
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.content_type.startswith('text/csv'))
            
            # Check CSV content
            csv_data = response.data.decode('utf-8')
            lines = csv_data.strip().split('\n')
            self.assertEqual(len(lines), 4)  # Header + 3 records
    
    def test_export_etl_log(self):
        """Test ETL log export"""
        with patch('fund_etl_ui.DB_PATH', str(self.test_db)):
            response = self.app.get('/api/export/etl-log')
            
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.content_type.startswith('text/csv'))


class TestAPIIntegration(ETLTestCase):
    """Test integration between UI and ETL APIs"""
    
    @patch('requests.get')
    def test_workflow_polling(self, mock_get):
        """Test workflow status polling"""
        # Mock ETL API responses
        responses = [
            {'status': 'running', 'output': [{'message': 'Starting'}]},
            {'status': 'running', 'output': [{'message': 'Starting'}, 
                                            {'message': 'Processing'}]},
            {'status': 'completed', 'output': [{'message': 'Starting'}, 
                                              {'message': 'Processing'},
                                              {'message': 'Complete'}]}
        ]
        
        mock_get.side_effect = [
            Mock(status_code=200, json=lambda: resp) for resp in responses
        ]
        
        # Import the polling function
        from fund_etl_ui import poll_etl_workflow
        
        # Mock workflow tracker
        mock_tracker = Mock()
        
        with patch('fund_etl_ui.workflow_tracker', mock_tracker):
            # Run polling in thread
            poll_thread = threading.Thread(
                target=poll_etl_workflow,
                args=('ui-123', 'etl-456')
            )
            poll_thread.daemon = True
            poll_thread.start()
            
            # Give it time to poll
            time.sleep(0.5)
            
            # Should have updated workflow status
            self.assertTrue(mock_tracker.update_workflow.called)
    
    def test_error_handling_chain(self):
        """Test error handling through API chain"""
        # Test connection error handling
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.exceptions.ConnectionError()
            
            from fund_etl_ui import app as ui_app
            app = ui_app.test_client()
            response = app.post('/api/workflow/run-daily')
            
            self.assertEqual(response.status_code, 503)
            data = json.loads(response.data)
            self.assertIn('Unable to connect', data['error'])


if __name__ == '__main__':
    unittest.main(verbosity=2)