#!/usr/bin/env python3
"""
Database Tests for Fund ETL System
Tests database initialization, operations, and integrity
"""

import unittest
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from test_framework import ETLTestCase
from fund_etl_pipeline import FundDataETL
from fund_etl_utilities import FundDataMonitor
from workflow_db_tracker import DatabaseWorkflowTracker


class TestDatabaseInitialization(ETLTestCase):
    """Test database initialization and schema"""
    
    def test_create_database(self):
        """Test database creation with all tables"""
        conn = self.create_test_database()
        cursor = conn.cursor()
        
        # Check all expected tables exist
        cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        ORDER BY name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = ['etl_log', 'etl_runs', 'fund_data', 'workflows']
        
        for table in expected_tables:
            self.assertIn(table, tables, f"Table {table} not found")
        
        conn.close()
    
    def test_fund_data_schema(self):
        """Test fund_data table schema"""
        conn = self.create_test_database()
        cursor = conn.cursor()
        
        # Get table info
        cursor.execute("PRAGMA table_info(fund_data)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        # Check critical columns
        expected_columns = {
            'id': 'INTEGER',
            'region': 'TEXT',
            'date': 'TEXT',
            'fund_code': 'TEXT',
            'fund_name': 'TEXT',
            'share_class_assets': 'REAL',
            'portfolio_assets': 'REAL',
            'one_day_yield': 'REAL',
            'seven_day_yield': 'REAL',
            'daily_liquidity': 'REAL'
        }
        
        for col, dtype in expected_columns.items():
            self.assertIn(col, columns, f"Column {col} not found")
            self.assertEqual(columns[col], dtype, 
                           f"Column {col} has wrong type: {columns[col]} != {dtype}")
        
        conn.close()
    
    def test_indices(self):
        """Test database indices are created"""
        conn = self.create_test_database()
        cursor = conn.cursor()
        
        # Get all indices
        cursor.execute("""
        SELECT name, tbl_name FROM sqlite_master 
        WHERE type='index' AND name NOT LIKE 'sqlite_%'
        """)
        
        indices = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Check critical indices exist
        expected_indices = [
            'idx_fund_data_region',
            'idx_fund_data_date',
            'idx_fund_data_fund_code',
            'idx_fund_data_composite'
        ]
        
        for idx in expected_indices:
            self.assertIn(idx, indices, f"Index {idx} not found")
        
        conn.close()


class TestDatabaseOperations(ETLTestCase):
    """Test database CRUD operations"""
    
    def setUp(self):
        super().setUp()
        self.conn = self.create_test_database()
    
    def tearDown(self):
        self.conn.close()
        super().tearDown()
    
    def test_insert_fund_data(self):
        """Test inserting fund data"""
        # Insert test data
        count = self.insert_test_data(self.conn, num_records=5)
        
        # Verify insertion
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM fund_data")
        actual_count = cursor.fetchone()[0]
        
        self.assertEqual(actual_count, 5, "Wrong number of records inserted")
        
        # Verify data integrity
        cursor.execute("""
        SELECT fund_code, fund_name, share_class_assets 
        FROM fund_data 
        WHERE fund_code = 'TEST0001'
        """)
        
        row = cursor.fetchone()
        self.assertIsNotNone(row, "Test record not found")
        self.assertEqual(row[0], 'TEST0001')
        self.assertEqual(row[1], 'Test Fund 1')
        self.assertEqual(row[2], 1100000.0)
    
    def test_update_fund_data(self):
        """Test updating existing fund data"""
        # Insert initial data
        self.insert_test_data(self.conn, date='2024-01-15')
        
        # Update a record
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE fund_data 
        SET share_class_assets = 9999999.99,
            one_day_yield = 0.05
        WHERE fund_code = 'TEST0001'
        """)
        self.conn.commit()
        
        # Verify update
        cursor.execute("""
        SELECT share_class_assets, one_day_yield 
        FROM fund_data 
        WHERE fund_code = 'TEST0001'
        """)
        
        row = cursor.fetchone()
        self.assertEqual(row[0], 9999999.99)
        self.assertEqual(row[1], 0.05)
    
    def test_etl_log_insertion(self):
        """Test ETL log tracking"""
        cursor = self.conn.cursor()
        
        # Insert ETL log entry
        cursor.execute("""
        INSERT INTO etl_log (
            run_date, region, file_date, status, 
            records_processed, download_time, processing_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            '2024-01-15', 'AMRS', '2024-01-15', 'SUCCESS',
            1500, 5.5, 10.2
        ))
        self.conn.commit()
        
        # Verify insertion
        cursor.execute("SELECT * FROM etl_log WHERE region = 'AMRS'")
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        self.assertEqual(row[2], 'AMRS')  # region
        self.assertEqual(row[4], 'SUCCESS')  # status
        self.assertEqual(row[5], 1500)  # records_processed
    
    def test_duplicate_handling(self):
        """Test handling of duplicate records"""
        # Insert initial data
        self.insert_test_data(self.conn, date='2024-01-15')
        
        # Try to insert duplicate (should update, not insert new)
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR REPLACE INTO fund_data (
            region, date, fund_code, fund_name,
            share_class_assets
        ) VALUES (?, ?, ?, ?, ?)
        """, (
            'AMRS', '2024-01-15', 'TEST0001',
            'Test Fund 1 Updated', 8888888.88
        ))
        self.conn.commit()
        
        # Should still have same number of records
        cursor.execute("SELECT COUNT(*) FROM fund_data")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 10)
        
        # But with updated values
        cursor.execute("""
        SELECT fund_name, share_class_assets 
        FROM fund_data 
        WHERE fund_code = 'TEST0001'
        """)
        row = cursor.fetchone()
        self.assertEqual(row[0], 'Test Fund 1 Updated')
        self.assertEqual(row[1], 8888888.88)


class TestWorkflowDatabase(ETLTestCase):
    """Test workflow database operations"""
    
    def setUp(self):
        super().setUp()
        self.conn = self.create_test_database()
        self.tracker = DatabaseWorkflowTracker(str(self.test_db))
    
    def tearDown(self):
        self.conn.close()
        super().tearDown()
    
    def test_workflow_creation(self):
        """Test creating workflow records"""
        # Start a workflow
        workflow_id = self.tracker.start_workflow('test-workflow', {'param': 'value'})
        
        self.assertIsNotNone(workflow_id)
        self.assertTrue(len(workflow_id) > 0)
        
        # Verify in database
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM workflows WHERE id = ?", (workflow_id,))
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        self.assertEqual(row[1], 'test-workflow')  # type
        self.assertEqual(row[2], 'pending')  # status
    
    def test_workflow_updates(self):
        """Test updating workflow status and output"""
        # Create workflow
        workflow_id = self.tracker.start_workflow('test-workflow')
        
        # Update to running
        self.tracker.update_workflow(workflow_id, status='running')
        
        # Add output
        self.tracker.update_workflow(workflow_id, output_line='Processing started')
        self.tracker.update_workflow(workflow_id, output_line='Step 1 complete')
        
        # Complete workflow
        self.tracker.update_workflow(workflow_id, status='completed', 
                                   message='All done')
        
        # Verify updates
        workflow = self.tracker.get_workflow(workflow_id)
        
        self.assertEqual(workflow['status'], 'completed')
        self.assertEqual(workflow['message'], 'All done')
        self.assertEqual(len(workflow['output']), 2)
        self.assertIsNotNone(workflow['completed_at'])
    
    def test_workflow_persistence(self):
        """Test workflow data persists across tracker instances"""
        # Create workflow with first tracker
        workflow_id = self.tracker.start_workflow('persist-test')
        self.tracker.update_workflow(workflow_id, status='running')
        self.tracker.update_workflow(workflow_id, output_line='Test output')
        
        # Create new tracker instance
        new_tracker = DatabaseWorkflowTracker(str(self.test_db))
        
        # Should be able to retrieve workflow
        workflow = new_tracker.get_workflow(workflow_id)
        
        self.assertIsNotNone(workflow)
        self.assertEqual(workflow['type'], 'persist-test')
        self.assertEqual(workflow['status'], 'running')
        self.assertEqual(len(workflow['output']), 1)
    
    def test_workflow_cleanup(self):
        """Test cleaning up old workflows"""
        # Create old workflows
        for i in range(5):
            wf_id = self.tracker.start_workflow(f'old-workflow-{i}')
            self.tracker.update_workflow(wf_id, status='completed')
        
        # Manually set completed_at to old date
        cursor = self.conn.cursor()
        old_date = (datetime.now() - timedelta(days=2)).isoformat()
        cursor.execute("""
        UPDATE workflows 
        SET completed_at = ? 
        WHERE type LIKE 'old-workflow-%'
        """, (old_date,))
        self.conn.commit()
        
        # Create recent workflow
        recent_id = self.tracker.start_workflow('recent-workflow')
        self.tracker.update_workflow(recent_id, status='completed')
        
        # Clean up workflows older than 1 day
        deleted = self.tracker.cleanup_old_workflows(hours=24)
        
        self.assertEqual(deleted, 5)
        
        # Verify old workflows deleted
        all_workflows = self.tracker.get_all_workflows()
        self.assertEqual(len(all_workflows), 1)
        self.assertEqual(all_workflows[0]['type'], 'recent-workflow')


class TestDataIntegrity(ETLTestCase):
    """Test data integrity and constraints"""
    
    def setUp(self):
        super().setUp()
        self.conn = self.create_test_database()
    
    def tearDown(self):
        self.conn.close()
        super().tearDown()
    
    def test_date_consistency(self):
        """Test date fields are consistent"""
        # Insert data with different dates
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO fund_data (
            region, date, fund_code, fund_name
        ) VALUES (?, ?, ?, ?, ?)
        """, ('AMRS', '2024-01-15', 'TEST001', 'Test Fund'))
        self.conn.commit()
        
        # Verify dates match
        cursor.execute("""
        SELECT date FROM fund_data WHERE fund_code = 'TEST001'
        """)
        row = cursor.fetchone()
        
        self.assertEqual(row[0], '2024-01-15', "date should be correct")
    
    def test_numeric_precision(self):
        """Test numeric fields maintain precision"""
        cursor = self.conn.cursor()
        
        # Insert precise values
        test_values = {
            'share_class_assets': 12345678.90,
            'portfolio_assets': 98765432.10,
            'one_day_yield': 0.0123,
            'seven_day_yield': 0.0456,
            'daily_liquidity': 0.7890
        }
        
        cursor.execute("""
        INSERT INTO fund_data (
            region, date, fund_code, fund_name,
            share_class_assets, portfolio_assets,
            one_day_yield, seven_day_yield, daily_liquidity
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            'AMRS', '2024-01-15', 'PRECISION', 'Precision Test',
            test_values['share_class_assets'],
            test_values['portfolio_assets'],
            test_values['one_day_yield'],
            test_values['seven_day_yield'],
            test_values['daily_liquidity']
        ))
        self.conn.commit()
        
        # Retrieve and verify
        cursor.execute("""
        SELECT share_class_assets, portfolio_assets,
               one_day_yield, seven_day_yield, daily_liquidity
        FROM fund_data WHERE fund_code = 'PRECISION'
        """)
        row = cursor.fetchone()
        
        self.assertEqual(row[0], test_values['share_class_assets'])
        self.assertEqual(row[1], test_values['portfolio_assets'])
        self.assertAlmostEqual(row[2], test_values['one_day_yield'], places=4)
        self.assertAlmostEqual(row[3], test_values['seven_day_yield'], places=4)
        self.assertAlmostEqual(row[4], test_values['daily_liquidity'], places=4)
    
    def test_null_handling(self):
        """Test handling of NULL values"""
        cursor = self.conn.cursor()
        
        # Insert with some NULL values
        cursor.execute("""
        INSERT INTO fund_data (
            region, date, fund_code, fund_name,
            share_class_assets, portfolio_assets
        ) VALUES (?, ?, ?, ?, ?, ?)
        """, ('AMRS', '2024-01-15', 'NULL_TEST', 'Null Test Fund', None, None))
        self.conn.commit()
        
        # Retrieve and verify NULLs
        cursor.execute("""
        SELECT share_class_assets, portfolio_assets, one_day_yield
        FROM fund_data WHERE fund_code = 'NULL_TEST'
        """)
        row = cursor.fetchone()
        
        self.assertIsNone(row[0])
        self.assertIsNone(row[1])
        self.assertIsNone(row[2])


if __name__ == '__main__':
    unittest.main(verbosity=2)