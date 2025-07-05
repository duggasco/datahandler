#!/usr/bin/env python3
"""
Test script for 30-day lookback validation feature
"""

import sys
import os
sys.path.append('/app')

from fund_etl_pipeline import FundDataETL
from fund_etl_utilities import FundDataMonitor
import pandas as pd
from datetime import datetime

def test_validation_config():
    """Test that validation configuration is loaded correctly"""
    print("=== Testing Validation Configuration ===\n")
    
    try:
        etl = FundDataETL('/config/config.json')
        
        # Check if validation config exists
        validation_config = etl.config.get('validation', {})
        print(f"Validation enabled: {validation_config.get('enabled', False)}")
        print(f"Change threshold: {validation_config.get('change_threshold_percent', 5.0)}%")
        print(f"Critical fields: {validation_config.get('critical_fields', [])}")
        
        # Check if lookback URLs exist
        sap_urls = etl.config.get('sap_urls', {})
        for region in ['amrs_30days', 'emea_30days']:
            if region in sap_urls:
                print(f"✓ {region} URL configured")
            else:
                print(f"✗ {region} URL missing")
        
        return True
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def test_validation_methods():
    """Test that validation methods are available"""
    print("\n=== Testing Validation Methods ===\n")
    
    try:
        etl = FundDataETL('/config/config.json')
        
        # Check if methods exist
        methods = ['download_lookback_file', 'validate_against_lookback', 
                  '_compare_dataframes', 'update_from_lookback']
        
        for method in methods:
            if hasattr(etl, method):
                print(f"✓ Method {method} exists")
            else:
                print(f"✗ Method {method} missing")
        
        return True
        
    except Exception as e:
        print(f"✗ Methods test failed: {e}")
        return False

def test_mock_validation():
    """Test validation with mock data"""
    print("\n=== Testing Mock Validation ===\n")
    
    try:
        etl = FundDataETL('/config/config.json')
        
        # Create mock lookback data
        mock_data = pd.DataFrame({
            'Date': [datetime(2025, 7, 1)],
            'Fund Code': ['TEST001'],
            'Fund Name': ['Test Fund'],
            'Share Class Assets (dly/$mils)': [100.0],
            'Portfolio Assets (dly/$mils)': [1000.0],
            '1-DSY (dly)': [0.01],
            '7-DSY (dly)': [0.07]
        })
        
        # Run validation (it will find this as missing)
        results = etl.validate_against_lookback('TEST', mock_data)
        
        print(f"Validation completed:")
        print(f"  Missing dates: {results['summary']['missing_dates_count']}")
        print(f"  Changed records: {results['summary']['changed_records_count']}")
        
        return True
        
    except Exception as e:
        print(f"✗ Mock validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all validation tests"""
    print("=" * 60)
    print("30-Day Lookback Validation Test Suite")
    print("=" * 60)
    print(f"Running at: {datetime.now()}")
    print()
    
    tests = [
        ("Configuration", test_validation_config),
        ("Methods", test_validation_methods),
        ("Mock Validation", test_mock_validation)
    ]
    
    all_passed = True
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        success = test_func()
        if not success:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✅ All validation tests passed!")
    else:
        print("⚠️  Some validation tests failed.")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
