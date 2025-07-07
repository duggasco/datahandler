#!/usr/bin/env python3
"""
Check ETL history to understand why database might be empty
"""

import sys
import os
sys.path.append('/app')

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def check_etl_history():
    """Check ETL run history to understand database state"""
    print("=== ETL History Analysis ===\n")
    
    db_path = '/data/fund_data.db'
    conn = sqlite3.connect(db_path)
    
    # 1. Check ETL log
    print("1. ETL RUN HISTORY (Last 30 days)")
    print("-" * 80)
    
    etl_query = """
    SELECT 
        run_date,
        region,
        file_date,
        status,
        records_processed,
        issues,
        created_at
    FROM etl_log
    ORDER BY created_at DESC
    LIMIT 50
    """
    
    etl_history = pd.read_sql_query(etl_query, conn)
    
    if len(etl_history) == 0:
        print("No ETL history found - database may be newly created")
    else:
        print(f"Found {len(etl_history)} ETL runs")
        print("\nRecent ETL runs:")
        print(etl_history.to_string(index=False))
    
    # 2. Check success vs failure rates
    print("\n2. ETL SUCCESS/FAILURE SUMMARY")
    print("-" * 60)
    
    status_query = """
    SELECT 
        status,
        COUNT(*) as count,
        SUM(COALESCE(records_processed, 0)) as total_records
    FROM etl_log
    GROUP BY status
    """
    
    status_summary = pd.read_sql_query(status_query, conn)
    print(status_summary.to_string(index=False))
    
    # 3. Check data distribution by date
    print("\n3. DATA DISTRIBUTION BY DATE")
    print("-" * 60)
    
    date_dist_query = """
    SELECT 
        date,
        region,
        COUNT(*) as record_count
    FROM fund_data
    GROUP BY date, region
    ORDER BY date DESC
    LIMIT 20
    """
    
    date_dist = pd.read_sql_query(date_dist_query, conn)
    
    if len(date_dist) == 0:
        print("❌ NO DATA FOUND IN fund_data TABLE!")
        print("\nThis explains why all lookback records are being treated as new.")
        print("The database appears to be empty or nearly empty.")
    else:
        print(f"Data distribution (last 20 date/region combinations):")
        print(date_dist.to_string(index=False))
        
        # Calculate total by region
        region_totals = date_dist.groupby('region')['record_count'].sum()
        print(f"\nTotal records by region:")
        for region, total in region_totals.items():
            print(f"  {region}: {total:,}")
    
    # 4. Check for any successful loads
    print("\n4. SUCCESSFUL DATA LOADS")
    print("-" * 60)
    
    success_query = """
    SELECT 
        run_date,
        region,
        records_processed,
        created_at
    FROM etl_log
    WHERE status = 'SUCCESS' AND records_processed > 0
    ORDER BY created_at DESC
    LIMIT 10
    """
    
    successes = pd.read_sql_query(success_query, conn)
    
    if len(successes) == 0:
        print("❌ No successful data loads found in ETL history!")
        print("This confirms the database has never been properly populated.")
    else:
        print(f"Last {len(successes)} successful loads:")
        print(successes.to_string(index=False))
    
    # 5. Check for specific issues
    print("\n5. RECENT ETL ISSUES")
    print("-" * 60)
    
    issues_query = """
    SELECT 
        run_date,
        region,
        status,
        issues
    FROM etl_log
    WHERE status IN ('FAILED', 'CARRIED_FORWARD') 
    AND issues IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 10
    """
    
    issues = pd.read_sql_query(issues_query, conn)
    
    if len(issues) > 0:
        print("Recent issues:")
        for _, row in issues.iterrows():
            print(f"\n{row['run_date']} - {row['region']} ({row['status']}):")
            print(f"  {row['issues']}")
    else:
        print("No recent issues found")
    
    # 6. Recommendations
    print("\n6. ANALYSIS & RECOMMENDATIONS")
    print("-" * 60)
    
    total_records = pd.read_sql_query("SELECT COUNT(*) as count FROM fund_data", conn).iloc[0]['count']
    
    if total_records == 0:
        print("❌ DATABASE IS EMPTY")
        print("\nRecommended actions:")
        print("1. Run initial data load:")
        print("   ./run-etl.sh run")
        print("\n2. Or load historical data:")
        print("   ./run-etl.sh historical 2025-06-01 2025-07-07")
        print("\n3. After loading data, validation should work correctly")
        
    elif total_records < 1000:
        print(f"⚠️  DATABASE HAS ONLY {total_records} RECORDS")
        print("\nThis is unusually low. Recommended actions:")
        print("1. Check if initial load completed successfully")
        print("2. Review ETL logs for download failures")
        print("3. Consider reloading historical data")
        
    else:
        print(f"✓ Database contains {total_records:,} records")
        print("\nIf validation is still failing:")
        print("1. Check for fund code format mismatches")
        print("2. Verify region codes match (AMRS vs US)")
        print("3. Run comprehensive diagnostic:")
        print("   ./run-etl.sh diagnose-comprehensive")
    
    conn.close()
    print("\n=== End ETL History Analysis ===")

if __name__ == "__main__":
    check_etl_history()
