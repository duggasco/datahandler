#!/usr/bin/env python3
"""
Initialize database with initial data load
"""

import sys
import os
sys.path.append('/app')

from fund_etl_pipeline import FundDataETL
from fund_etl_scheduler import ETLScheduler
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def initialize_database():
    """Initialize empty database with data"""
    print("=== Database Initialization Tool ===\n")
    
    etl = FundDataETL('/config/config.json')
    conn = sqlite3.connect(etl.db_path)
    
    # Check current state
    total_records = pd.read_sql_query("SELECT COUNT(*) as count FROM fund_data", conn).iloc[0]['count']
    
    print(f"Current database status: {total_records:,} records\n")
    
    if total_records > 1000:
        print("✓ Database already contains data.")
        response = input("\nDo you want to continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Initialization cancelled.")
            return
    
    # Determine date range
    end_date = datetime.now().date()
    
    # For initial load, we'll go back 7 days to get a good baseline
    start_date = end_date - timedelta(days=7)
    
    print(f"\nInitialization plan:")
    print(f"- Load data from {start_date} to {end_date}")
    print(f"- Regions: AMRS and EMEA")
    print(f"- This will download and process approximately 14 files")
    
    response = input("\nProceed with initialization? (y/N): ")
    if response.lower() != 'y':
        print("Initialization cancelled.")
        return
    
    conn.close()
    
    # Run the initialization
    print("\nStarting initialization...")
    scheduler = ETLScheduler()
    
    current_date = start_date
    success_count = 0
    fail_count = 0
    
    while current_date <= end_date:
        if etl.is_business_day(datetime.combine(current_date, datetime.min.time())):
            print(f"\nProcessing {current_date}...")
            
            try:
                # Run ETL for this date
                result = etl.run_daily_etl(datetime.combine(current_date, datetime.min.time()))
                
                if isinstance(result, dict) and result.get('success'):
                    print(f"✓ Successfully loaded data for {current_date}")
                    success_count += 1
                else:
                    print(f"✗ Failed to load data for {current_date}")
                    fail_count += 1
                    
            except Exception as e:
                print(f"✗ Error processing {current_date}: {str(e)}")
                fail_count += 1
        else:
            print(f"\nSkipping {current_date} (weekend/holiday)")
        
        current_date += timedelta(days=1)
    
    # Final summary
    print("\n" + "="*60)
    print("Initialization Complete")
    print("="*60)
    
    # Check final state
    conn = sqlite3.connect(etl.db_path)
    final_records = pd.read_sql_query("SELECT COUNT(*) as count FROM fund_data", conn).iloc[0]['count']
    final_regions = pd.read_sql_query(
        "SELECT region, COUNT(*) as count FROM fund_data GROUP BY region", 
        conn
    )
    
    print(f"\nResults:")
    print(f"- Successful loads: {success_count}")
    print(f"- Failed loads: {fail_count}")
    print(f"- Total records in database: {final_records:,}")
    
    print(f"\nRecords by region:")
    for _, row in final_regions.iterrows():
        print(f"  {row['region']}: {row['count']:,}")
    
    if final_records > 0:
        print("\n✓ Database successfully initialized!")
        print("\nYou can now run validation:")
        print("  ./run-etl.sh validate")
    else:
        print("\n❌ Initialization failed - no data loaded")
        print("\nTroubleshooting steps:")
        print("1. Check SAP connectivity: ./run-etl.sh test")
        print("2. Check logs: ./run-etl.sh logs")
        print("3. Verify credentials in /config/config.json")
    
    conn.close()

if __name__ == "__main__":
    initialize_database()
