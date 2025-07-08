#!/usr/bin/env python3
"""
SAP Download and Validation Tests
Tests SAP download functionality and 30-day lookback validation
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json
import sqlite3
from unittest.mock import Mock, patch, MagicMock

from test_framework import ETLTestCase, MockSAPDownloader
from fund_etl_pipeline import FundDataETL
from sap_download_module import SAPOpenDocumentDownloader


class TestSAPDownloader(ETLTestCase):
    """Test SAP download functionality"""
    
    def setUp(self):
        super().setUp()
        self.downloader = MockSAPDownloader(self.data_dir)
    
    def test_download_daily_file(self):
        """Test downloading daily data files"""
        # Test AMRS download
        target_date = datetime(2024, 1, 15)
        output_dir = self.data_dir / 'downloads'
        output_dir.mkdir(exist_ok=True)
        
        filepath = self.downloader.download_file('AMRS', target_date, output_dir)
        
        self.assertIsNotNone(filepath)
        self.assertTrue(Path(filepath).exists())
        self.assertTrue(filepath.endswith('DataDump__AMRS_20240115.xlsx'))
        
        # Verify file content
        df = pd.read_excel(filepath)
        self.assertGreater(len(df), 0)
        self.assertIn('Fund Code', df.columns)
    
    def test_download_lookback_file(self):
        """Test downloading 30-day lookback files"""
        target_date = datetime(2024, 1, 15)
        output_dir = self.data_dir / 'downloads'
        output_dir.mkdir(exist_ok=True)
        
        # Test lookback file (should have more records)
        filepath = self.downloader.download_file(
            'AMRS_30DAYS', target_date, output_dir
        )
        
        self.assertIsNotNone(filepath)
        df = pd.read_excel(filepath)
        self.assertGreater(len(df), 1000)  # Lookback files are larger
    
    def test_download_failure_handling(self):
        """Test handling of download failures"""
        self.downloader.should_fail = True
        
        target_date = datetime(2024, 1, 15)
        output_dir = self.data_dir / 'downloads'
        output_dir.mkdir(exist_ok=True)
        
        filepath = self.downloader.download_file('AMRS', target_date, output_dir)
        
        self.assertIsNone(filepath)
    
    def test_connectivity_check(self):
        """Test SAP connectivity check"""
        # Test successful connectivity
        results = self.downloader.test_connectivity()
        
        self.assertTrue(results['AMRS'])
        self.assertTrue(results['EMEA'])
        self.assertTrue(results['AMRS 30DAYS'])
        self.assertTrue(results['EMEA 30DAYS'])
        
        # Test failed connectivity
        self.downloader.should_fail = True
        results = self.downloader.test_connectivity()
        
        self.assertFalse(results['AMRS'])
        self.assertFalse(results['EMEA'])


class TestSAPConfiguration(ETLTestCase):
    """Test SAP downloader configuration"""
    
    def test_url_configuration(self):
        """Test URL configuration handling"""
        config = {
            'username': 'test_user',
            'password': 'test_pass',
            'sap_urls': {
                'AMRS': 'https://test.com/amrs',
                'EMEA': 'https://test.com/emea',
                'amrs_30days': 'https://test.com/amrs_lookback',  # lowercase
                'EMEA_30DAYS': 'https://test.com/emea_lookback'  # uppercase
            }
        }
        
        # Mock the downloader initialization
        with patch('sap_download_module.webdriver.Chrome'):
            downloader = SAPOpenDocumentDownloader(config)
            
            # Should normalize keys to uppercase
            self.assertIn('AMRS', downloader.urls)
            self.assertIn('EMEA', downloader.urls)
            self.assertIn('AMRS_30DAYS', downloader.urls)
            self.assertIn('EMEA_30DAYS', downloader.urls)
            
            # Check URL values
            self.assertEqual(downloader.urls['AMRS'], 'https://test.com/amrs')
    
    def test_timeout_configuration(self):
        """Test timeout configuration"""
        config = {
            'username': 'test_user',
            'password': 'test_pass',
            'timeout': 600,
            'lookback_timeout': 1200
        }
        
        with patch('sap_download_module.webdriver.Chrome'):
            downloader = SAPOpenDocumentDownloader(config)
            
            self.assertEqual(downloader.timeout, 600)
            self.assertEqual(downloader.lookback_timeout, 1200)


class TestValidationLogic(ETLTestCase):
    """Test 30-day lookback validation"""
    
    def setUp(self):
        super().setUp()
        config_path = self.create_test_config()
        self.etl = FundDataETL(config_path)
        self.etl.setup_database()
        self.conn = sqlite3.connect(self.etl.db_path)
    
    def tearDown(self):
        self.conn.close()
        super().tearDown()
    
    def create_lookback_data(self, region='AMRS', days=30):
        """Create mock lookback data for testing"""
        dates = []
        base_date = datetime(2024, 1, 15)
        
        for i in range(days):
            date = base_date - timedelta(days=i)
            if date.weekday() < 5:  # Weekdays only
                dates.append(date.strftime('%Y-%m-%d'))
        
        # Create DataFrame with multiple dates
        data_rows = []
        funds = [f'FUND{i:04d}' for i in range(100)]
        
        for date in dates:
            for fund in funds:
                data_rows.append({
                    'date': date,
                    'fund_code': fund,
                    'fund_name': f'Test Fund {fund}',
                    'share_class_assets': np.random.uniform(1e6, 1e8),
                    'portfolio_assets': np.random.uniform(2e6, 2e8),
                    'one_day_yield': np.random.uniform(0.001, 0.05),
                    'seven_day_yield': np.random.uniform(0.002, 0.06),
                    'daily_liquidity': np.random.uniform(0.3, 0.8)
                })
        
        return pd.DataFrame(data_rows)
    
    def test_validate_against_lookback(self):
        """Test validation against lookback data"""
        # Insert current data
        self.insert_test_data(self.conn, 'AMRS', '2024-01-15', 50)
        
        # Create lookback data with some differences
        lookback_df = self.create_lookback_data('AMRS', days=30)
        
        # Modify some values to create differences
        mask = lookback_df['date'] == '2024-01-15'
        lookback_df.loc[mask & (lookback_df['fund_code'] == 'FUND0001'), 
                       'share_class_assets'] = 99999999
        
        # Run validation
        results = self.etl.validate_against_lookback('AMRS', lookback_df)
        
        self.assertIn('summary', results)
        self.assertIn('missing_dates', results)
        self.assertIn('changed_records', results)
        
        # Should detect the change
        self.assertGreater(results['summary']['changed_records_count'], 0)
    
    def test_missing_dates_detection(self):
        """Test detection of missing dates in current data"""
        # Insert data for only some dates
        self.insert_test_data(self.conn, 'AMRS', '2024-01-15', 50)
        self.insert_test_data(self.conn, 'AMRS', '2024-01-12', 50)
        # Missing 2024-01-11
        
        # Create complete lookback data
        lookback_df = self.create_lookback_data('AMRS', days=5)
        
        # Run validation
        results = self.etl.validate_against_lookback('AMRS', lookback_df)
        
        # Should find missing dates
        self.assertGreater(results['summary']['missing_dates_count'], 0)
        self.assertIn('2024-01-11', results['missing_dates'])
    
    def test_change_threshold(self):
        """Test change threshold detection"""
        # Insert current data
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO fund_data (
            region, date, fund_code, fund_name,
            share_class_assets, one_day_yield
        ) VALUES (?, ?, ?, ?, ?, ?)
        """, ('AMRS', '2024-01-15', 'THRESHOLD_TEST', 'Threshold Test Fund',
              1000000, 0.0100))
        self.conn.commit()
        
        # Create lookback with significant change
        lookback_df = pd.DataFrame([{
            'date': '2024-01-15',
            'fund_code': 'THRESHOLD_TEST',
            'fund_name': 'Threshold Test Fund',
            'share_class_assets': 1000000,
            'one_day_yield': 0.0200  # 100% change in yield
        }])
        
        # Set threshold in config
        self.etl.config['validation']['change_threshold'] = 0.05  # 5%
        
        # Run validation
        results = self.etl.validate_against_lookback('AMRS', lookback_df)
        
        # Should flag the large change
        changes = [c for c in results['changed_records'] 
                  if c['fund_code'] == 'THRESHOLD_TEST']
        self.assertEqual(len(changes), 1)
        
        change = changes[0]
        self.assertIn('one_day_yield', change['changes'])
        self.assertGreater(abs(change['changes']['one_day_yield']['pct_change']), 0.05)


class TestValidationModes(ETLTestCase):
    """Test selective vs full validation update modes"""
    
    def setUp(self):
        super().setUp()
        config_path = self.create_test_config()
        self.etl = FundDataETL(config_path)
        self.etl.setup_database()
        self.conn = sqlite3.connect(self.etl.db_path)
    
    def tearDown(self):
        self.conn.close()
        super().tearDown()
    
    def test_selective_update_mode(self):
        """Test selective update mode - only updates changed records"""
        # Insert initial data
        for i in range(10):
            cursor = self.conn.cursor()
            cursor.execute("""
            INSERT INTO fund_data (
                region, date, fund_code, fund_name,
                share_class_assets, one_day_yield
            ) VALUES (?, ?, ?, ?, ?, ?)
            """, ('AMRS', '2024-01-15', f'FUND{i:04d}', f'Test Fund {i}',
                  1000000 + i * 100000, 0.01 + i * 0.001))
        self.conn.commit()
        
        # Create lookback with some changes
        lookback_data = []
        for i in range(10):
            lookback_data.append({
                'date': '2024-01-15',
                'fund_code': f'FUND{i:04d}',
                'fund_name': f'Test Fund {i}',
                'share_class_assets': 1000000 + i * 100000,
                'one_day_yield': 0.01 + i * 0.001
            })
        
        # Change only first 3 records
        for i in range(3):
            lookback_data[i]['share_class_assets'] = 9999999
        
        lookback_df = pd.DataFrame(lookback_data)
        
        # Run validation
        results = self.etl.validate_against_lookback('AMRS', lookback_df)
        
        # Update with selective mode
        update_result = self.etl.update_from_lookback(
            'AMRS', lookback_df, results, update_mode='selective'
        )
        
        # Only 3 records should be updated
        self.assertEqual(update_result['records_updated'], 3)
        
        # Verify in database
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT COUNT(*) FROM fund_data 
        WHERE share_class_assets = 9999999
        """)
        count = cursor.fetchone()[0]
        self.assertEqual(count, 3)
    
    def test_full_update_mode(self):
        """Test full update mode - replaces all records for dates"""
        # Insert initial data for multiple dates
        for date in ['2024-01-15', '2024-01-12']:
            self.insert_test_data(self.conn, 'AMRS', date, 5)
        
        # Create lookback data for one date only
        lookback_data = []
        for i in range(5):
            lookback_data.append({
                'date': '2024-01-15',
                'fund_code': f'NEWFUND{i:04d}',  # All different codes
                'fund_name': f'New Fund {i}',
                'share_class_assets': 5555555,
                'one_day_yield': 0.05
            })
        
        lookback_df = pd.DataFrame(lookback_data)
        
        # Run validation
        results = self.etl.validate_against_lookback('AMRS', lookback_df)
        
        # Update with full mode
        update_result = self.etl.update_from_lookback(
            'AMRS', lookback_df, results, update_mode='full'
        )
        
        # Should replace all records for 2024-01-15
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT COUNT(*) FROM fund_data 
        WHERE region = 'AMRS' AND date = '2024-01-15'
        """)
        count = cursor.fetchone()[0]
        self.assertEqual(count, 5)
        
        # All should be new funds
        cursor.execute("""
        SELECT COUNT(*) FROM fund_data 
        WHERE region = 'AMRS' AND date = '2024-01-15' 
        AND fund_code LIKE 'NEWFUND%'
        """)
        count = cursor.fetchone()[0]
        self.assertEqual(count, 5)
        
        # Other date should be unchanged
        cursor.execute("""
        SELECT COUNT(*) FROM fund_data 
        WHERE region = 'AMRS' AND date = '2024-01-12'
        """)
        count = cursor.fetchone()[0]
        self.assertEqual(count, 5)


class TestValidationReporting(ETLTestCase):
    """Test validation reporting functionality"""
    
    def setUp(self):
        super().setUp()
        config_path = self.create_test_config()
        self.etl = FundDataETL(config_path)
        self.etl.setup_database()
    
    def test_validation_summary(self):
        """Test validation summary generation"""
        # Create mock validation results
        results = {
            'summary': {
                'total_lookback_records': 1000,
                'missing_dates_count': 2,
                'changed_records_count': 50,
                'requires_update': True
            },
            'missing_dates': ['2024-01-11', '2024-01-10'],
            'changed_records': [
                {
                    'fund_code': 'FUND001',
                    'date': '2024-01-15',
                    'type': 'value_change',
                    'changes': {
                        'share_class_assets': {
                            'current': 1000000,
                            'lookback': 1100000,
                            'pct_change': 0.10
                        }
                    }
                }
            ]
        }
        
        # Format summary for display
        summary_text = self.etl._format_validation_summary(results)
        
        self.assertIn('Total lookback records: 1000', summary_text)
        self.assertIn('Missing dates: 2', summary_text)
        self.assertIn('Changed records: 50', summary_text)
    
    def test_change_details_formatting(self):
        """Test formatting of change details"""
        change_record = {
            'fund_code': 'TEST001',
            'date': '2024-01-15',
            'type': 'value_change',
            'changes': {
                'share_class_assets': {
                    'current': 1000000,
                    'lookback': 1100000,
                    'pct_change': 0.10
                },
                'one_day_yield': {
                    'current': 0.01,
                    'lookback': 0.015,
                    'pct_change': 0.50
                }
            }
        }
        
        # Format change details
        details = []
        for field, change_info in change_record['changes'].items():
            details.append(
                f"{field}: {change_info['current']} → {change_info['lookback']} "
                f"({change_info['pct_change']*100:.1f}% change)"
            )
        
        self.assertEqual(len(details), 2)
        self.assertIn('share_class_assets', details[0])
        self.assertIn('10.0% change', details[0])


if __name__ == '__main__':
    unittest.main(verbosity=2)