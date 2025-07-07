#!/usr/bin/env python3
"""
Comprehensive diagnostic to identify why all records are being flagged as new
"""

import sys
import os
sys.path.append('/app')

from fund_etl_pipeline import FundDataETL
import pandas as pd
import sqlite3
from datetime import datetime, timedelta

def comprehensive_diagnostic():
    """Run comprehensive diagnostic of database vs lookback file mismatch"""
    print("=== Comprehensive Fund ETL Diagnostic ===\n")
    
    etl = FundDataETL('/config/config.json')
    conn = sqlite3.connect(etl.db_path)
    
    # 1. Check database status
    print("1. DATABASE STATUS CHECK")
    print("-" * 60)
    
    # Count total records
    total_query = "SELECT COUNT(*) as count, COUNT(DISTINCT fund_code) as funds FROM fund_data"
    result = pd.read_sql_query(total_query, conn)
    print(f"Total records in database: {result.iloc[0]['count']:,}")
    print(f"Unique fund codes: {result.iloc[0]['funds']:,}")
    
    # Check date range
    date_query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM fund_data"
    dates = pd.read_sql_query(date_query, conn)
    print(f"Date range: {dates.iloc[0]['min_date']} to {dates.iloc[0]['max_date']}")
    
    # Check regions
    region_query = "SELECT region, COUNT(*) as count FROM fund_data GROUP BY region"
    regions = pd.read_sql_query(region_query, conn)
    print("\nRecords by region:")
    for _, row in regions.iterrows():
        print(f"  {row['region']}: {row['count']:,}")
    
    # Sample fund codes from database
    print("\n2. SAMPLE FUND CODES FROM DATABASE")
    print("-" * 60)
    
    sample_query = """
    SELECT DISTINCT fund_code, fund_name 
    FROM fund_data 
    WHERE region = 'AMRS' 
    ORDER BY fund_code 
    LIMIT 10
    """
    db_samples = pd.read_sql_query(sample_query, conn)
    print("First 10 fund codes in database (AMRS):")
    for _, row in db_samples.iterrows():
        print(f"  '{row['fund_code']}' - {row['fund_name']}")
    
    # 3. Download and check lookback file
    print("\n3. LOOKBACK FILE ANALYSIS")
    print("-" * 60)
    
    region = 'AMRS'
    print(f"Downloading lookback file for {region}...")
    lookback_df = etl.download_lookback_file(region)
    
    if lookback_df is None:
        print("Failed to download lookback file")
        conn.close()
        return
    
    print(f"Lookback file has {len(lookback_df)} records")
    
    # Clean fund codes in lookback
    lookback_df['Fund Code'] = lookback_df['Fund Code'].apply(
        lambda x: x.strip() if isinstance(x, str) else x
    )
    
    # Sample fund codes from lookback
    print("\nFirst 10 fund codes in lookback file:")
    for i, (_, row) in enumerate(lookback_df.head(10).iterrows()):
        print(f"  '{row['Fund Code']}' - {row['Fund Name']}")
    
    # 4. Compare fund codes
    print("\n4. FUND CODE COMPARISON")
    print("-" * 60)
    
    # Get all AMRS fund codes from database for any date
    db_funds_query = """
    SELECT DISTINCT fund_code 
    FROM fund_data 
    WHERE region = 'AMRS'
    """
    db_funds = pd.read_sql_query(db_funds_query, conn)['fund_code'].tolist()
    
    # Get unique fund codes from lookback
    lookback_funds = lookback_df['Fund Code'].unique().tolist()
    
    print(f"Database has {len(db_funds)} unique AMRS fund codes")
    print(f"Lookback has {len(lookback_funds)} unique fund codes")
    
    # Find overlaps
    db_set = set(db_funds)
    lookback_set = set(lookback_funds)
    
    common_funds = db_set.intersection(lookback_set)
    db_only = db_set - lookback_set
    lookback_only = lookback_set - db_set
    
    print(f"\nFund code overlap analysis:")
    print(f"  Common funds: {len(common_funds)}")
    print(f"  In database only: {len(db_only)}")
    print(f"  In lookback only: {len(lookback_only)}")
    
    if len(common_funds) == 0:
        print("\n⚠️  NO COMMON FUND CODES FOUND!")
        print("This explains why all records are being treated as new.")
        
        # Show examples of mismatches
        if len(db_only) > 0 and len(lookback_only) > 0:
            print("\nExamples of fund codes that don't match:")
            print("Database examples:")
            for fc in list(db_only)[:5]:
                print(f"  '{fc}' (length: {len(fc)})")
            print("Lookback examples:")
            for fc in list(lookback_only)[:5]:
                print(f"  '{fc}' (length: {len(fc)})")
    
    # 5. Check specific dates
    print("\n5. DATE-SPECIFIC ANALYSIS")
    print("-" * 60)
    
    # Get a date that exists in both
    lookback_dates = lookback_df['Date'].dt.date.unique()
    sample_date = lookback_dates[0]  # Use first date from lookback
    
    print(f"Checking date: {sample_date}")
    
    # Get database records for this date
    date_query = f"""
    SELECT fund_code, fund_name, share_class_assets, one_day_yield
    FROM fund_data 
    WHERE date = '{sample_date}' AND region = 'AMRS'
    LIMIT 5
    """
    db_date_data = pd.read_sql_query(date_query, conn)
    
    print(f"\nDatabase records for {sample_date} (first 5):")
    if len(db_date_data) > 0:
        for _, row in db_date_data.iterrows():
            print(f"  {row['fund_code']}: {row['fund_name']}")
    else:
        print("  NO RECORDS FOUND FOR THIS DATE")
    
    # Get lookback records for same date
    lookback_date_data = lookback_df[lookback_df['Date'].dt.date == sample_date].head(5)
    print(f"\nLookback records for {sample_date} (first 5):")
    for _, row in lookback_date_data.iterrows():
        print(f"  {row['Fund Code']}: {row['Fund Name']}")
    
    # 6. Check for formatting issues
    print("\n6. FORMATTING ANALYSIS")
    print("-" * 60)
    
    if len(db_funds) > 0 and len(lookback_funds) > 0:
        # Check for whitespace issues
        db_sample = db_funds[0] if db_funds else ""
        lookback_sample = lookback_funds[0] if lookback_funds else ""
        
        print(f"Sample database fund code: '{db_sample}' (length: {len(db_sample)})")
        print(f"Sample lookback fund code: '{lookback_sample}' (length: {len(lookback_sample)})")
        
        # Check for common patterns
        print("\nChecking for common patterns:")
        
        # Case differences
        db_upper = [f.upper() for f in db_funds[:10]]
        lookback_upper = [f.upper() for f in lookback_funds[:10]]
        if set(db_upper).intersection(set(lookback_upper)):
            print("  ⚠️  Found matches when ignoring case!")
        
        # Leading/trailing spaces
        db_stripped = [f.strip() for f in db_funds[:10]]
        lookback_stripped = [f.strip() for f in lookback_funds[:10]]
        if len(set(db_stripped).intersection(set(lookback_stripped))) > len(common_funds):
            print("  ⚠️  Found more matches after stripping whitespace!")
    
    # 7. Recommendations
    print("\n7. DIAGNOSTIC SUMMARY & RECOMMENDATIONS")
    print("-" * 60)
    
    if len(common_funds) == 0:
        print("❌ CRITICAL ISSUE: No matching fund codes between database and lookback file")
        print("\nPossible causes:")
        print("1. Database is empty or contains test data")
        print("2. Database was populated from a different data source")
        print("3. Fund codes have been transformed during loading")
        print("4. Region mismatch (AMRS vs US data)")
        
        print("\nRecommended actions:")
        print("1. Check if initial data load was successful")
        print("2. Verify the data source for existing database records")
        print("3. Consider clearing database and reloading from scratch")
        print("4. Check ETL logs for any transformation issues")
    
    elif len(common_funds) < len(lookback_funds) * 0.1:
        print(f"⚠️  WARNING: Only {len(common_funds)} matching funds (less than 10%)")
        print("Most records will be treated as new")
    
    else:
        print(f"✓ Found {len(common_funds)} matching fund codes")
        print("Validation should work normally for these funds")
    
    conn.close()
    print("\n=== End Diagnostic ===")

if __name__ == "__main__":
    comprehensive_diagnostic()
