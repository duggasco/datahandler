#!/usr/bin/env python3
"""
Tests for the transform_data method
Separated from test_etl_core.py to avoid syntax issues
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

from test_framework import ETLTestCase
from fund_etl_pipeline import FundDataETL


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
            'Share Class Assets (dly/$mils)': ['1,234,567.89', '2,345,678.90'],
            'Portfolio Assets (dly/$mils)': ['2,345,678.90', '3,456,789.01'],
            '1-DSY (dly)': ['0.0123', '0.0234'],
            '7-DSY (dly)': ['0.0456', '0.0567'],
            'Daily Liquidity (%)': ['0.50', '0.60']
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
        self.assertEqual(transformed.iloc[0]['date'], '2024-01-15')
        self.assertEqual(transformed.iloc[0]['region'], 'AMRS')
    
    def test_handle_missing_values(self):
        """Test handling of missing/null values"""
        raw_data = pd.DataFrame({
            'Fund Code': ['FUND001'],
            'Fund Name': ['Test Fund'],
            'Share Class Assets (dly/$mils)': [np.nan],  # Missing value
            'Portfolio Assets (dly/$mils)': [''],  # Empty string
            '1-DSY (dly)': ['N/A'],  # Invalid value
            '7-DSY (dly)': [None],  # None value
            'Daily Liquidity (%)': ['0.50']
        })
        
        transformed = self.etl.transform_data(
            raw_data, 'AMRS', datetime(2024, 1, 15)
        )
        
        # Should handle missing values gracefully
        self.assertTrue(pd.isna(transformed.iloc[0]['share_class_assets']))
        self.assertTrue(pd.isna(transformed.iloc[0]['portfolio_assets']))
        self.assertTrue(pd.isna(transformed.iloc[0]['one_day_yield']))
        self.assertTrue(pd.isna(transformed.iloc[0]['seven_day_yield']))
        self.assertEqual(transformed.iloc[0]['daily_liquidity'], 0.50)
    
    def test_multivalue_handling(self):
        """Test handling of #MULTIVALUE entries"""
        raw_data = pd.DataFrame({
            'Fund Code': ['FUND001', '#MULTIVALUE'],
            'Fund Name': ['Test Fund 1', 'Multi Fund'],
            'Share Class Assets (dly/$mils)': ['1,000,000', '2,000,000'],
            'Portfolio Assets (dly/$mils)': ['2,000,000', '3,000,000'],
            '1-DSY (dly)': ['0.01', '0.02'],
            '7-DSY (dly)': ['0.02', '0.03'],
            'Daily Liquidity (%)': ['0.50', '0.60']
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
            'Date': ['2024-01-15'],
            'Fund Code': ['FUND001'],
            'Fund Name': ['Test Fund'],
            'Share Class Assets (dly/$mils)': ['1,000,000'],
            'Portfolio Assets (dly/$mils)': ['2,000,000'],
            '1-DSY (dly)': ['0.01'],
            '7-DSY (dly)': ['0.02'],
            'Daily Liquidity (%)': ['0.50']
        })
        
        transformed = self.etl.transform_data(raw_data, 'AMRS', test_date)
        
        # Check date fields
        self.assertEqual(transformed.iloc[0]['date'], '2024-01-15')
        self.assertEqual(transformed.iloc[0]['file_date'], '2024-01-15')
    
    def test_weekend_data_handling(self):
        """Test weekend data uses Friday's date"""
        config_path = self.create_test_config()
        etl = FundDataETL(config_path)
        etl.setup_database()
        
        # Saturday should use Friday's data
        saturday = datetime(2024, 1, 13)
        friday = datetime(2024, 1, 12)
        
        # Create test data with Friday's date
        raw_data = pd.DataFrame({
            'Date': ['2024-01-12'],  # Friday
            'Fund Code': ['FUND001'],
            'Fund Name': ['Test Fund'],
            'Share Class Assets (dly/$mils)': ['1,000,000'],
            'Portfolio Assets (dly/$mils)': ['2,000,000'],
            '1-DSY (dly)': ['0.01'],
            '7-DSY (dly)': ['0.02'],
            'Daily Liquidity (%)': ['0.50']
        })
        
        # Transform as if it's Saturday
        transformed = etl.transform_data(raw_data, 'AMRS', saturday)
        
        # Date should be Saturday but file_date is Friday
        self.assertEqual(transformed.iloc[0]['date'], '2024-01-13')
        self.assertEqual(transformed.iloc[0]['file_date'], '2024-01-12')


class TestDataValidation(ETLTestCase):
    """Test data validation functionality"""
    
    def setUp(self):
        super().setUp()
        config_path = self.create_test_config()
        self.etl = FundDataETL(config_path)
        self.etl.setup_database()
    
    def test_validate_required_columns(self):
        """Test validation of required DataFrame columns"""
        # Missing required columns
        bad_df = pd.DataFrame({
            'Fund Code': ['TEST001'],
            'Fund Name': ['Test Fund']
            # Missing other required columns
        })
        
        # Should handle gracefully in transform
        result = self.etl.transform_data(
            bad_df, 'AMRS', datetime(2024, 1, 15)
        )
        
        # Should have transformed available columns
        self.assertIn('fund_code', result.columns)
        self.assertIn('fund_name', result.columns)
        self.assertEqual(result.iloc[0]['fund_code'], 'TEST001')
        # Missing columns should not be in result
        self.assertNotIn('share_class_assets', result.columns)
    
    def test_validate_numeric_ranges(self):
        """Test validation of numeric value ranges"""
        test_data = pd.DataFrame({
            'Fund Code': ['TEST001', 'TEST002'],
            'Fund Name': ['Test Fund 1', 'Test Fund 2'],
            'Share Class Assets (dly/$mils)': ['1000000', '-500000'],  # Negative value
            'Portfolio Assets (dly/$mils)': ['2000000', '3000000'],
            '1-DSY (dly)': ['0.01', '1.50'],  # Unrealistic yield
            '7-DSY (dly)': ['0.02', '0.03'],
            'Daily Liquidity (%)': ['0.50', '1.50']  # > 100%
        })
        
        transformed = self.etl.transform_data(
            test_data, 'AMRS', datetime(2024, 1, 15)
        )
        
        # Should handle invalid values appropriately
        # Negative assets should be preserved (no validation in transform)
        self.assertEqual(transformed.iloc[1]['share_class_assets'], -500000)
        
        # Extreme yields are also preserved (no validation in transform)
        self.assertEqual(transformed.iloc[1]['one_day_yield'], 1.50)
        
        # Values over 100% are preserved
        self.assertEqual(transformed.iloc[1]['daily_liquidity'], 1.50)


if __name__ == '__main__':
    unittest.main(verbosity=2)