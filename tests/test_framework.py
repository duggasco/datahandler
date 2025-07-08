#!/usr/bin/env python3
"""
Test Framework for Fund ETL System
Provides base classes and utilities for comprehensive testing
"""

import unittest
import sqlite3
import tempfile
import shutil
import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
import time
import requests
from typing import Dict, List, Optional, Tuple

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class ETLTestCase(unittest.TestCase):
    """Base test case class with common setup and utilities"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests"""
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        cls.logger = logging.getLogger(cls.__name__)
        
        # Create temporary directories
        cls.temp_dir = tempfile.mkdtemp(prefix='etl_test_')
        cls.data_dir = Path(cls.temp_dir) / 'data'
        cls.logs_dir = Path(cls.temp_dir) / 'logs'
        cls.config_dir = Path(cls.temp_dir) / 'config'
        
        # Create directories
        for dir_path in [cls.data_dir, cls.logs_dir, cls.config_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Test database path
        cls.test_db = cls.data_dir / 'test_fund_data.db'
        
        cls.logger.info(f"Test environment created at: {cls.temp_dir}")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        try:
            shutil.rmtree(cls.temp_dir)
            cls.logger.info("Test environment cleaned up")
        except Exception as e:
            cls.logger.error(f"Error cleaning up test environment: {e}")
    
    def setUp(self):
        """Set up for each test"""
        # Create fresh database for each test
        if self.test_db.exists():
            self.test_db.unlink()
        
        # Clear logs
        for log_file in self.logs_dir.glob('*.log'):
            log_file.unlink()
    
    def create_test_config(self, additional_config: Dict = None) -> str:
        """Create a test configuration file"""
        config = {
            "db_path": str(self.test_db),
            "data_dir": str(self.data_dir),
            "download_dir": str(self.data_dir / 'downloads'),
            "log_dir": str(self.logs_dir),
            "sap_config": {
                "username": "test_user",
                "password": "test_pass",
                "timeout": 30,
                "headless": True
            },
            "validation": {
                "update_mode": "selective",
                "change_threshold": 0.05
            }
        }
        
        if additional_config:
            config.update(additional_config)
        
        config_path = self.config_dir / 'test_config.json'
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        return str(config_path)
    
    def create_test_database(self) -> sqlite3.Connection:
        """Create and initialize test database"""
        from fund_etl_pipeline import FundDataETL
        
        # Create ETL instance with test config
        config_path = self.create_test_config()
        etl = FundDataETL(config_path)
        
        # Initialize tables
        etl.setup_database()
        
        return sqlite3.connect(str(self.test_db))
    
    def insert_test_data(self, conn: sqlite3.Connection, 
                        region: str = 'AMRS', 
                        date: str = '2024-01-15',
                        num_records: int = 10) -> int:
        """Insert test data into database"""
        cursor = conn.cursor()
        
        for i in range(num_records):
            cursor.execute("""
            INSERT INTO fund_data (
                region, date, fund_code, fund_name,
                share_class_assets, portfolio_assets, 
                one_day_yield, seven_day_yield, daily_liquidity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                region,
                date,
                f'TEST{i:04d}',
                f'Test Fund {i}',
                1000000.0 + (i * 100000),
                2000000.0 + (i * 200000),
                0.01 + (i * 0.001),
                0.02 + (i * 0.001),
                0.50 + (i * 0.01)
            ))
        
        conn.commit()
        return cursor.lastrowid
    
    def assert_database_exists(self):
        """Assert that test database exists and has tables"""
        self.assertTrue(self.test_db.exists(), "Test database does not exist")
        
        conn = sqlite3.connect(str(self.test_db))
        cursor = conn.cursor()
        
        # Check for tables
        cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name IN ('fund_data', 'etl_log', 'etl_runs', 'workflows')
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        self.assertIn('fund_data', tables, "fund_data table missing")
        self.assertIn('etl_log', tables, "etl_log table missing")
        
        conn.close()
    
    def get_record_count(self, table: str, where_clause: str = "") -> int:
        """Get count of records in a table"""
        conn = sqlite3.connect(str(self.test_db))
        cursor = conn.cursor()
        
        query = f"SELECT COUNT(*) FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        cursor.execute(query)
        count = cursor.fetchone()[0]
        conn.close()
        
        return count
    
    def wait_for_condition(self, condition_func, timeout: int = 30, 
                          interval: float = 0.5) -> bool:
        """Wait for a condition to become true"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if condition_func():
                return True
            time.sleep(interval)
        
        return False


class MockSAPDownloader:
    """Mock SAP downloader for testing without actual SAP connection"""
    
    def __init__(self, test_data_dir: Path):
        self.test_data_dir = test_data_dir
        self.download_count = 0
        self.should_fail = False
        self.delay = 0
    
    def download_file(self, region: str, target_date: datetime, 
                     output_dir: Path) -> Optional[str]:
        """Mock download that creates test Excel files"""
        if self.should_fail:
            return None
        
        if self.delay:
            time.sleep(self.delay)
        
        # Create mock Excel file
        import pandas as pd
        
        filename = f"DataDump__{region}_{target_date.strftime('%Y%m%d')}.xlsx"
        filepath = output_dir / filename
        
        # Create test data
        num_records = 100 if '30DAYS' not in region else 3000
        
        data = {
            'Fund Code': [f'FUND{i:04d}' for i in range(num_records)],
            'Fund Name': [f'Test Fund {i}' for i in range(num_records)],
            'Share Class Assets': [1000000 + (i * 10000) for i in range(num_records)],
            'Portfolio Assets': [2000000 + (i * 20000) for i in range(num_records)],
            '1 Day Yield': [0.01 + (i * 0.0001) for i in range(num_records)],
            '7 Day Yield': [0.02 + (i * 0.0001) for i in range(num_records)],
            'Daily Liquidity': [0.50 + (i * 0.001) for i in range(num_records)]
        }
        
        df = pd.DataFrame(data)
        df.to_excel(filepath, index=False)
        
        self.download_count += 1
        return str(filepath)
    
    def test_connectivity(self) -> Dict[str, bool]:
        """Mock connectivity test"""
        return {
            'AMRS': not self.should_fail,
            'EMEA': not self.should_fail,
            'AMRS 30DAYS': not self.should_fail,
            'EMEA 30DAYS': not self.should_fail
        }
    
    def close(self):
        """Mock close method"""
        pass


class APITestMixin:
    """Mixin for testing API endpoints"""
    
    @property
    def base_url(self) -> str:
        """Base URL for API tests"""
        return "http://localhost:8080"
    
    @property
    def api_url(self) -> str:
        """API URL for ETL service"""
        return "http://localhost:8081"
    
    def api_get(self, endpoint: str, timeout: int = 10) -> requests.Response:
        """Make GET request to API"""
        url = f"{self.base_url}{endpoint}"
        return requests.get(url, timeout=timeout)
    
    def api_post(self, endpoint: str, data: Dict = None, 
                timeout: int = 10) -> requests.Response:
        """Make POST request to API"""
        url = f"{self.base_url}{endpoint}"
        return requests.post(url, json=data, timeout=timeout)
    
    def assert_api_success(self, response: requests.Response, 
                          expected_status: int = 200):
        """Assert API request was successful"""
        self.assertEqual(response.status_code, expected_status,
                        f"API returned {response.status_code}: {response.text}")
    
    def wait_for_workflow(self, workflow_id: str, 
                         expected_status: str = 'completed',
                         timeout: int = 60) -> Dict:
        """Wait for workflow to reach expected status"""
        def check_status():
            response = self.api_get(f"/api/workflow/status/{workflow_id}")
            if response.status_code == 200:
                workflow = response.json()
                return workflow.get('status') == expected_status
            return False
        
        if self.wait_for_condition(check_status, timeout):
            response = self.api_get(f"/api/workflow/status/{workflow_id}")
            return response.json()
        
        raise TimeoutError(f"Workflow {workflow_id} did not reach {expected_status} status")


def run_test_suite(test_modules: List[str] = None, verbosity: int = 2) -> bool:
    """Run the complete test suite"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    if test_modules:
        # Load specific test modules
        for module_name in test_modules:
            try:
                module = __import__(f'tests.{module_name}', fromlist=[module_name])
                suite.addTests(loader.loadTestsFromModule(module))
            except ImportError as e:
                print(f"Failed to load test module {module_name}: {e}")
    else:
        # Load all test modules
        test_dir = Path(__file__).parent
        for test_file in test_dir.glob('test_*.py'):
            if test_file.name == 'test_framework.py':
                continue
            
            module_name = test_file.stem
            try:
                module = __import__(f'tests.{module_name}', fromlist=[module_name])
                suite.addTests(loader.loadTestsFromModule(module))
            except ImportError as e:
                print(f"Failed to load test module {module_name}: {e}")
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    # Example usage
    print("Fund ETL Test Framework")
    print("=" * 50)
    print("This module provides base classes for testing.")
    print("Run specific test modules or use run_test_suite()")