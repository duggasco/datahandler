#!/usr/bin/env python3
"""
Core ETL Functionality Tests
Tests ETL pipeline operations, transformations, and business logic
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json
import sqlite3

from test_framework import ETLTestCase, MockSAPDownloader
from fund_etl_pipeline import FundDataETL
from fund_etl_utilities import FundDataMonitor, get_previous_business_day


class TestETLInitialization(ETLTestCase):
    """Test ETL initialization and configuration"""
    
    def test_etl_creation(self):
        """Test creating ETL instance"""
        config_path = self.create_test_config()
        etl = FundDataETL(config_path)
        
        self.assertIsNotNone(etl)
        self.assertEqual(etl.db_path, str(self.test_db))
        self.assertTrue(hasattr(etl, 'config'))
    
    def test_config_loading(self):
        """Test configuration file loading"""
        # Create config with custom values
        custom_config = {
            "sap_config": {
                "timeout": 600,
                "lookback_timeout": 1200
            },
            "validation": {
                "change_threshold": 0.10
            }
        }
        
        config_path = self.create_test_config(custom_config)
        etl = FundDataETL(config_path)
        
        self.assertEqual(etl.config['sap_config']['timeout'], 600)
        self.assertEqual(etl.config['validation']['change_threshold'], 0.10)
    
    def test_default_config(self):
        """Test ETL works with default configuration"""
        # Create minimal config
        config = {"database_path": str(self.test_db)}
        config_path = self.config_dir / 'minimal_config.json'
        
        with open(config_path, 'w') as f:
            json.dump(config, f)
        
        etl = FundDataETL(str(config_path))
        
        # Should have default values
        self.assertIn('download_dir', etl.config)
        self.assertIn('sap_config', etl.config)


class TestDataTransformation(ETLTestCase):
    """Test data transformation logic"""
    
    def setUp(self):
        super().setUp()
        config_path = self.create_test_config()
        self.etl = FundDataETL(config_path)
        self.etl.setup_database()
    
    def test_transform_basic_data(self):
        """Test basic data transformation"""
        # Create test DataFrame
        raw_data = pd.DataFrame({
            'Fund Code': ['FUND001', 'FUND002'],
            'Fund Name': ['Test Fund 1', 'Test Fund 2'],
            'Share Class Assets': ['1,234,567.89', '2,345,678.90'],
            'Portfolio Assets': ['2,345,678.90', '3,456,789.01'],
            '1 Day Yield': ['0.0123', '0.0234'],
            '7 Day Yield': ['0.0456', '0.0567'],
            'Daily Liquidity': ['0.50', '0.60']
        })
        
        # Transform data
        transformed = self.etl.transform_data(
            raw_data, 'AMRS', datetime(2024, 1, 15)
        )
        
        # Verify transformation
        self.assertEqual(len(transformed), 2)
        self.assertEqual(transformed.iloc[0]['fund_code'], 'FUND001')
        self.assertEqual(transformed.iloc[0]['share_class_assets'], 1234567.89)
        self.assertEqual(transformed.iloc[0]['one_day_yield'], 0.0123)
    
    def test_handle_missing_values(self):
        """Test handling of missing/null values"""
        raw_data = pd.DataFrame({
            'Fund Code': ['FUND001'],
            'Fund Name': ['Test Fund'],
            'Share Class Assets': [np.nan],  # Missing value
            'Portfolio Assets': [''],  # Empty string
            '1 Day Yield': ['N/A'],  # Invalid value
            '7 Day Yield': [None],  # None value
            'Daily Liquidity': ['0.50']
        })
        
        transformed = self.etl.transform_data(
            raw_data, 'AMRS', datetime(2024, 1, 15)
        )
        
        # Should handle missing values gracefully
        self.assertTrue(pd.isna(transformed.iloc[0]['share_class_assets']))
        self.assertTrue(pd.isna(transformed.iloc[0]['portfolio_assets']))
        self.assertEqual(transformed.iloc[0]['daily_liquidity'], 0.50)
    
    def test_multivalue_handling(self):
        """Test handling of #MULTIVALUE entries"""
        raw_data = pd.DataFrame({
            'Fund Code': ['FUND001', 'FUND002'],
            'Fund Name': ['Test Fund 1', '#MULTIVALUE'],
            'Share Class Assets': ['1,000,000', '#MULTIVALUE'],
            'Portfolio Assets': ['2,000,000', '3,000,000'],
            '1 Day Yield': ['0.01', '#MULTIVALUE'],
            '7 Day Yield': ['0.02', '0.03'],
            'Daily Liquidity': ['0.50', '0.60']
        })
        
        transformed = self.etl.transform_data(
            raw_data, 'EMEA', datetime(2024, 1, 15)
        )
        
        # MULTIVALUE rows should be filtered out
        self.assertEqual(len(transformed), 1)
        self.assertEqual(transformed.iloc[0]['fund_code'], 'FUND001')
    
    def test_date_formatting(self):
        """Test date field formatting"""
        test_date = datetime(2024, 1, 15)
        raw_data = pd.DataFrame({
            'Fund Code': ['FUND001'],
            'Fund Name': ['Test Fund'],
            'Share Class Assets': ['1,000,000'],
            'Portfolio Assets': ['2,000,000'],
            '1 Day Yield': ['0.01'],
            '7 Day Yield': ['0.02'],
            'Daily Liquidity': ['0.50']
        })
        
        transformed = self.etl.transform_data(raw_data, 'AMRS', test_date)
        
        # Check date fields
        self.assertEqual(transformed.iloc[0]['date'], '2024-01-15')


class TestBusinessDayLogic(ETLTestCase):
    """Test business day calculations and weekend handling"""
    
    def test_is_business_day(self):
        """Test business day detection"""
        etl = FundDataETL(self.create_test_config())
        
        # Weekday
        monday = datetime(2024, 1, 15)  # Monday
        self.assertTrue(etl.is_business_day(monday))
        
        # Weekend
        saturday = datetime(2024, 1, 13)  # Saturday
        sunday = datetime(2024, 1, 14)  # Sunday
        self.assertFalse(etl.is_business_day(saturday))
        self.assertFalse(etl.is_business_day(sunday))
        
        # US Holiday (New Year's Day 2024 was a Monday)
        new_year = datetime(2024, 1, 1)
        self.assertFalse(etl.is_business_day(new_year))
    
    def test_get_previous_business_day(self):
        """Test previous business day calculation"""
        # Monday -> Friday
        monday = datetime(2024, 1, 15)
        prev = get_previous_business_day(monday)
        self.assertEqual(prev.date(), datetime(2024, 1, 12).date())
        
        # Tuesday after holiday Monday (MLK Day)
        tuesday_after_holiday = datetime(2024, 1, 16)
        prev = get_previous_business_day(tuesday_after_holiday)
        # Should skip Monday holiday
        self.assertEqual(prev.date(), datetime(2024, 1, 12).date())
    
    def test_weekend_data_handling(self):
        """Test weekend data uses Friday's date"""
        config_path = self.create_test_config()
        etl = FundDataETL(config_path)
        etl.initialize_tables()
        
        # Create mock downloader
        mock_downloader = MockSAPDownloader(self.test_data_dir)
        
        # Saturday should use Friday's data
        saturday = datetime(2024, 1, 13)
        friday = datetime(2024, 1, 12)
        
        # Mock the download to return Friday's file
        file_path = mock_downloader.download_file(
            'AMRS', friday, self.data_dir / 'downloads'
        )
        
        # Process the file as if it's Saturday
        df = pd.read_excel(file_path)
        transformed = etl.transform_data(df, 'AMRS', saturday)
        
        # Date should be Saturday but data is from Friday
        self.assertEqual(transformed.iloc[0]['date'], '2024-01-13')
        self.assertEqual(transformed.iloc[0]['file_date'], '2024-01-12')


class TestDataLoading(ETLTestCase):
    """Test data loading into database"""
    
    def setUp(self):
        super().setUp()
        config_path = self.create_test_config()
        self.etl = FundDataETL(config_path)
        self.etl.setup_database()
        self.conn = sqlite3.connect(self.etl.db_path)
    
    def tearDown(self):
        self.conn.close()
        super().tearDown()
    
    def test_load_new_data(self):
        """Test loading new data into empty database"""
        # Create test data
        test_data = pd.DataFrame({
            'region': ['AMRS'] * 5,
            'date': ['2024-01-15'] * 5,
            'file_date': ['2024-01-15'] * 5,
            'fund_code': [f'FUND{i:03d}' for i in range(5)],
            'fund_name': [f'Test Fund {i}' for i in range(5)],
            'share_class_assets': [1000000 + i * 100000 for i in range(5)],
            'portfolio_assets': [2000000 + i * 200000 for i in range(5)],
            'one_day_yield': [0.01 + i * 0.001 for i in range(5)],
            'seven_day_yield': [0.02 + i * 0.001 for i in range(5)],
            'daily_liquidity': [0.50 + i * 0.01 for i in range(5)]
        })
        
        # Load data
        result = self.etl.load_data(test_data, 'AMRS', '2024-01-15')
        
        # Verify result
        self.assertTrue(result['success'])
        self.assertEqual(result['records_processed'], 5)
        self.assertEqual(result['records_inserted'], 5)
        self.assertEqual(result['records_updated'], 0)
        
        # Verify in database
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM fund_data")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 5)
    
    def test_update_existing_data(self):
        """Test updating existing records"""
        # Insert initial data
        self.insert_test_data(self.conn, date='2024-01-15', num_records=3)
        
        # Create updated data with changes
        updated_data = pd.DataFrame({
            'region': ['AMRS'] * 3,
            'date': ['2024-01-15'] * 3,
            'as_of_date': ['2024-01-15'] * 3,
            'file_date': ['2024-01-15'] * 3,
            'fund_code': ['TEST0000', 'TEST0001', 'TEST0002'],
            'fund_name': ['Test Fund 0 Updated', 'Test Fund 1', 'Test Fund 2'],
            'share_class_assets': [9999999, 1100000, 1200000],  # First one changed
            'portfolio_assets': [2000000, 2200000, 2400000],
            'one_day_yield': [0.05, 0.011, 0.012],  # First one changed
            'seven_day_yield': [0.02, 0.021, 0.022],
            'daily_liquidity': [0.50, 0.51, 0.52]
        })
        
        # Load updated data
        result = self.etl.load_data(updated_data, 'AMRS', '2024-01-15')
        
        # Should update existing records
        self.assertEqual(result['records_processed'], 3)
        self.assertEqual(result['records_updated'], 1)  # Only first record changed
        self.assertEqual(result['records_inserted'], 0)
        
        # Verify updates in database
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT fund_name, share_class_assets, one_day_yield 
        FROM fund_data 
        WHERE fund_code = 'TEST0000'
        """)
        row = cursor.fetchone()
        
        self.assertEqual(row[0], 'Test Fund 0 Updated')
        self.assertEqual(row[1], 9999999)
        self.assertEqual(row[2], 0.05)
    
    def test_mixed_insert_update(self):
        """Test loading data with both new and existing records"""
        # Insert some initial data
        self.insert_test_data(self.conn, date='2024-01-15', num_records=3)
        
        # Create data with both existing and new records
        mixed_data = pd.DataFrame({
            'region': ['AMRS'] * 5,
            'date': ['2024-01-15'] * 5,
            'file_date': ['2024-01-15'] * 5,
            'fund_code': ['TEST0001', 'TEST0002', 'NEW001', 'NEW002', 'NEW003'],
            'fund_name': ['Test Fund 1', 'Test Fund 2', 'New Fund 1', 
                         'New Fund 2', 'New Fund 3'],
            'share_class_assets': [1100000, 1200000, 5000000, 6000000, 7000000],
            'portfolio_assets': [2200000, 2400000, 8000000, 9000000, 10000000],
            'one_day_yield': [0.011, 0.012, 0.03, 0.04, 0.05],
            'seven_day_yield': [0.021, 0.022, 0.06, 0.07, 0.08],
            'daily_liquidity': [0.51, 0.52, 0.60, 0.65, 0.70]
        })
        
        # Load mixed data
        result = self.etl.load_data(mixed_data, 'AMRS', '2024-01-15')
        
        self.assertEqual(result['records_processed'], 5)
        self.assertEqual(result['records_inserted'], 3)  # 3 new records
        self.assertEqual(result['records_updated'], 0)   # 2 unchanged
        
        # Total records should be 6 (3 original + 3 new)
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM fund_data")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 6)


class TestETLMonitoring(ETLTestCase):
    """Test ETL monitoring and reporting"""
    
    def setUp(self):
        super().setUp()
        config_path = self.create_test_config()
        self.etl = FundDataETL(config_path)
        self.etl.setup_database()
        self.monitor = FundDataMonitor(self.etl.db_path)
        self.conn = sqlite3.connect(self.etl.db_path)
    
    def tearDown(self):
        self.conn.close()
        super().tearDown()
    
    def test_etl_status_tracking(self):
        """Test ETL run status tracking"""
        # Log some ETL runs
        cursor = self.conn.cursor()
        
        # Successful run
        cursor.execute("""
        INSERT INTO etl_log (
            run_date, region, file_date, status, 
            records_processed, download_time, processing_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ('2024-01-15', 'AMRS', '2024-01-15', 'SUCCESS', 1500, 5.5, 10.2))
        
        # Failed run
        cursor.execute("""
        INSERT INTO etl_log (
            run_date, region, file_date, status, 
            issues
        ) VALUES (?, ?, ?, ?, ?)
        """, ('2024-01-15', 'EMEA', '2024-01-15', 'FAILED', 
              'Connection timeout'))
        
        self.conn.commit()
        
        # Get ETL status
        status_df = self.monitor.get_etl_status(days=1)
        
        self.assertEqual(len(status_df), 2)
        
        # Check successful run
        amrs_row = status_df[status_df['region'] == 'AMRS'].iloc[0]
        self.assertEqual(amrs_row['status'], 'SUCCESS')
        self.assertEqual(amrs_row['records_processed'], 1500)
        
        # Check failed run
        emea_row = status_df[status_df['region'] == 'EMEA'].iloc[0]
        self.assertEqual(emea_row['status'], 'FAILED')
        self.assertEqual(emea_row['issues'], 'Connection timeout')
    
    def test_missing_dates_detection(self):
        """Test detection of missing data dates"""
        # Insert data with gaps
        cursor = self.conn.cursor()
        
        # Insert data for some dates
        for date in ['2024-01-10', '2024-01-11', '2024-01-15']:
            self.insert_test_data(self.conn, 'AMRS', date, 5)
            self.insert_test_data(self.conn, 'EMEA', date, 5)
        
        # Check for missing dates
        missing = self.monitor.find_missing_dates('2024-01-10', '2024-01-15')
        
        # Should find 2024-01-12 (Friday) missing
        self.assertIn('AMRS', missing)
        self.assertIn('2024-01-12', missing['AMRS'])
        self.assertIn('EMEA', missing)
        self.assertIn('2024-01-12', missing['EMEA'])
    
    def test_data_quality_report(self):
        """Test data quality reporting"""
        # Insert data with varying quality
        cursor = self.conn.cursor()
        
        # Good quality data
        for i in range(5):
            cursor.execute("""
            INSERT INTO fund_data (
                region, date, fund_code, fund_name,
                share_class_assets, portfolio_assets,
                one_day_yield, seven_day_yield, daily_liquidity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, ('AMRS', '2024-01-15', f'GOOD{i:03d}', f'Good Fund {i}',
                  1000000, 2000000, 0.01, 0.02, 0.50))
        
        # Poor quality data (missing values)
        for i in range(3):
            cursor.execute("""
            INSERT INTO fund_data (
                region, date, fund_code, fund_name,
                share_class_assets
            ) VALUES (?, ?, ?, ?, ?)
            """, ('AMRS', '2024-01-15', f'POOR{i:03d}', f'Poor Fund {i}',
                  1000000))
        
        self.conn.commit()
        
        # Generate quality report
        report = self.monitor.generate_data_quality_report()
        
        self.assertIn('Data Quality Report', report)
        self.assertIn('AMRS', report)
        # Should show quality issues with missing yield data


class TestDataValidation(ETLTestCase):
    """Test data validation functionality"""
    
    def setUp(self):
        super().setUp()
        config_path = self.create_test_config()
        self.etl = FundDataETL(config_path)
        self.etl.setup_database()
    
    def test_validate_required_columns(self):
        """Test validation of required DataFrame columns"""
        # Missing required column
        bad_df = pd.DataFrame({
            'Fund Code': ['TEST001'],
            'Fund Name': ['Test Fund']
            # Missing other required columns
        })
        
        # Should handle gracefully in transform
        with self.assertLogs(level='WARNING') as cm:
            result = self.etl.transform_data(
                bad_df, 'AMRS', datetime(2024, 1, 15)
            )
        
        # Should log warnings about missing columns
        self.assertTrue(any('Share Class Assets' in log for log in cm.output))
    
    def test_validate_numeric_ranges(self):
        """Test validation of numeric value ranges"""
        test_data = pd.DataFrame({
            'Fund Code': ['TEST001', 'TEST002'],
            'Fund Name': ['Test Fund 1', 'Test Fund 2'],
            'Share Class Assets': ['1000000', '-500000'],  # Negative value
            'Portfolio Assets': ['2000000', '3000000'],
            '1 Day Yield': ['0.01', '1.50'],  # Unrealistic yield
            '7 Day Yield': ['0.02', '0.03'],
            'Daily Liquidity': ['0.50', '1.50']  # > 100%
        })
        
        transformed = self.etl.transform_data(
            test_data, 'AMRS', datetime(2024, 1, 15)
        )
        
        # Should handle invalid values appropriately
        # Negative assets should be preserved for investigation
        self.assertEqual(transformed.iloc[1]['share_class_assets'], -500000)
        
        # But extreme yields might be capped or flagged
        self.assertEqual(transformed.iloc[1]['one_day_yield'], 1.50)


if __name__ == '__main__':
    unittest.main(verbosity=2)