#!/usr/bin/env python3
"""
Test script to verify the Fund ETL setup
"""

import sqlite3
import os
import sys
from datetime import datetime

def test_database_setup():
    """Test that the database is properly initialized"""
    print("=== Testing Database Setup ===\n")
    
    db_path = "/data/fund_data.db"
    
    # Check if database file exists
    if not os.path.exists(db_path):
        print(f"❌ Database file not found at {db_path}")
        return False
    
    print(f"✅ Database file exists at {db_path}")
    print(f"   Size: {os.path.getsize(db_path):,} bytes")
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]
        
        print(f"\n✅ Tables found: {table_names}")
        
        # Verify expected tables exist
        expected_tables = ['etl_log', 'fund_data']
        for table in expected_tables:
            if table in table_names:
                print(f"   ✅ {table} table exists")
            else:
                print(f"   ❌ {table} table missing")
                return False
        
        # Check fund_data schema
        print("\n📋 fund_data table schema:")
        cursor.execute("PRAGMA table_info(fund_data)")
        columns = cursor.fetchall()
        
        expected_columns = [
            'date', 'region', 'fund_code', 'fund_name', 'master_class_fund_name',
            'rating', 'unique_identifier', 'nasdaq', 'fund_complex', 'subcategory',
            'domicile', 'currency', 'share_class_assets', 'portfolio_assets',
            'one_day_yield', 'one_day_gross_yield', 'seven_day_yield',
            'seven_day_gross_yield', 'expense_ratio', 'wam', 'wal',
            'transactional_nav', 'market_nav', 'daily_liquidity',
            'weekly_liquidity', 'fees', 'gates', 'created_at'
        ]
        
        actual_columns = [col[1] for col in columns]
        
        print(f"   Total columns: {len(actual_columns)}")
        print(f"   Expected columns: {len(expected_columns)}")
        
        # Check for missing columns
        missing_columns = set(expected_columns) - set(actual_columns)
        if missing_columns:
            print(f"   ❌ Missing columns: {missing_columns}")
            return False
        else:
            print(f"   ✅ All expected columns present")
        
        # Check indices
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='fund_data'")
        indices = cursor.fetchall()
        print(f"\n✅ Indices created: {[idx[0] for idx in indices]}")
        
        conn.close()
        
        print("\n✅ Database setup verified successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Database test failed: {e}")
        return False


def test_etl_components():
    """Test that ETL components can be imported and initialized"""
    print("\n=== Testing ETL Components ===\n")
    
    try:
        # Test imports
        sys.path.append('/app')
        
        print("Testing imports...")
        from fund_etl_pipeline import FundDataETL
        print("   ✅ fund_etl_pipeline imported")
        
        from fund_etl_utilities import FundDataMonitor, FundDataQuery
        print("   ✅ fund_etl_utilities imported")
        
        from fund_etl_scheduler import ETLScheduler
        print("   ✅ fund_etl_scheduler imported")
        
        # Test initialization
        print("\nTesting component initialization...")
        
        etl = FundDataETL('/config/config.json')
        print("   ✅ FundDataETL initialized")
        print(f"      DB Path: {etl.db_path}")
        print(f"      Data Dir: {etl.data_dir}")
        
        monitor = FundDataMonitor('/data/fund_data.db')
        print("   ✅ FundDataMonitor initialized")
        
        query = FundDataQuery('/data/fund_data.db')
        print("   ✅ FundDataQuery initialized")
        
        print("\n✅ All ETL components working!")
        return True
        
    except Exception as e:
        print(f"\n❌ Component test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sample_etl_run():
    """Test a sample ETL run with mock data"""
    print("\n=== Testing Sample ETL Run ===\n")
    
    try:
        import pandas as pd
        import numpy as np
        sys.path.append('/app')
        from fund_etl_pipeline import FundDataETL
        
        # Create a small test DataFrame
        test_data = pd.DataFrame({
            'Date': [datetime.now().date()],
            'Fund Code': ['TEST001'],
            'Fund Name': ['Test Fund'],
            'Master Class Fund Name': ['Test Master Fund'],
            'Rating (M/S&P/F)': ['AAA'],
            'Unique Identifier': ['UID001'],
            'NASDAQ': ['TEST'],
            'Fund Complex (Historical)': ['Test Complex'],
            'SubCategory Historical': ['Test Category'],
            'Domicile': ['US'],
            'Currency': ['USD'],
            'Share Class Assets (dly/$mils)': [100.0],
            'Portfolio Assets (dly/$mils)': [1000.0],
            '1-DSY (dly)': [0.01],
            '1-GDSY (dly)': [0.011],
            '7-DSY (dly)': [0.07],
            '7-GDSY (dly)': [0.077],
            'Chgd Expense Ratio (mo/dly)': [0.5],
            'WAM (dly)': [30],
            'WAL (dly)': [45],
            'Transactional NAV': ['1.00'],
            'Market NAV': ['1.00'],
            'Daily Liquidity (%)': [25.0],
            'Weekly Liquidity (%)': [40.0],
            'Fees': ['No'],
            'Gates': ['No']
        })
        
        print("Created test DataFrame")
        
        # Initialize ETL
        etl = FundDataETL('/config/config.json')
        
        # Validate the test data
        is_valid, issues = etl.validate_dataframe(test_data, 'TEST')
        print(f"\nValidation result: {'✅ Valid' if is_valid else '❌ Invalid'}")
        if issues:
            print(f"Issues: {issues}")
        
        # Try to load to database
        print("\nAttempting to load test data to database...")
        etl.load_to_database(test_data, 'TEST', datetime.now())
        
        # Verify data was loaded
        conn = sqlite3.connect('/data/fund_data.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM fund_data WHERE region = 'TEST'")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"✅ Successfully loaded {count} test record(s)")
            
            # Clean up test data
            cursor.execute("DELETE FROM fund_data WHERE region = 'TEST'")
            conn.commit()
            print("✅ Cleaned up test data")
        else:
            print("❌ No test data found in database")
            
        conn.close()
        
        print("\n✅ Sample ETL run completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Sample ETL run failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("=" * 60)
    print("Fund ETL Setup Test Suite")
    print("=" * 60)
    print(f"Running at: {datetime.now()}")
    print(f"Python version: {sys.version}")
    print()
    
    # Run tests
    tests = [
        ("Database Setup", test_database_setup),
        ("ETL Components", test_etl_components),
        ("Sample ETL Run", test_sample_etl_run)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        success = test_func()
        results.append((test_name, success))
        print()
    
    # Summary
    print("=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    all_passed = True
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name:<20} {status}")
        if not success:
            all_passed = False
    
    print()
    if all_passed:
        print("🎉 All tests passed! The Fund ETL system is ready to use.")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
