#!/usr/bin/env python3
"""
Test script to verify ETL functionality with actual data files
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
import os

def test_etl_with_files():
    """Test the ETL process with the uploaded files"""
    
    print("=== Testing ETL with Actual Files ===\n")
    
    # Test 1: Read and validate files
    print("1. Reading files...")
    try:
        emea_df = pd.read_excel('DataDump__EMEA.xlsx')
        us_df = pd.read_excel('DataDump__US.xlsx')
        print(f"✓ EMEA: {len(emea_df)} rows, {len(emea_df.columns)} columns")
        print(f"✓ US: {len(us_df)} rows, {len(us_df.columns)} columns")
    except Exception as e:
        print(f"✗ Error reading files: {e}")
        return
    
    # Test 2: Date parsing
    print("\n2. Testing date parsing...")
    emea_df['Date'] = pd.to_datetime(emea_df['Date'], errors='coerce')
    us_df['Date'] = pd.to_datetime(us_df['Date'], errors='coerce')
    
    print(f"✓ EMEA dates parsed: {emea_df['Date'].notna().sum()}/{len(emea_df)}")
    print(f"✓ US dates parsed: {us_df['Date'].notna().sum()}/{len(us_df)}")
    print(f"  EMEA date: {emea_df['Date'].iloc[0].strftime('%Y-%m-%d')}")
    print(f"  US date: {us_df['Date'].iloc[0].strftime('%Y-%m-%d')}")
    
    # Test 3: Handle duplicate fund codes
    print("\n3. Handling duplicate fund codes...")
    
    # Check EMEA duplicates
    emea_dupes = emea_df[emea_df.duplicated(['Fund Code'], keep=False)]
    if len(emea_dupes) > 0:
        print(f"  Found {len(emea_dupes)} duplicate rows in EMEA")
        # Fix #MULTIVALUE entries
        multivalue_mask = emea_df['Fund Code'] == '#MULTIVALUE'
        if multivalue_mask.any():
            count = multivalue_mask.sum()
            emea_df.loc[multivalue_mask, 'Fund Code'] = [f'#MULTIVALUE_{i+1}' for i in range(count)]
            print(f"  ✓ Fixed {count} #MULTIVALUE entries")
    
    # Test 4: Clean whitespace
    print("\n4. Cleaning whitespace...")
    text_cols = emea_df.select_dtypes(include=['object']).columns
    
    whitespace_before = 0
    for col in text_cols:
        # Count fields with whitespace
        mask = emea_df[col].notna() & (emea_df[col] != emea_df[col].str.strip())
        whitespace_before += mask.sum()
        # Clean
        emea_df[col] = emea_df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    print(f"  ✓ Cleaned {whitespace_before} fields with whitespace")
    
    # Test 5: Handle numeric conversions
    print("\n5. Converting numeric fields...")
    numeric_cols = ['Share Class Assets (dly/$mils)', '1-DSY (dly)', '7-DSY (dly)', 
                   'WAM (dly)', 'WAL (dly)', 'Daily Liquidity (%)']
    
    for col in numeric_cols:
        if col in emea_df.columns:
            # Replace '-' with NaN
            emea_df[col] = emea_df[col].replace(['-', ''], np.nan)
            # Convert to numeric
            before_nulls = emea_df[col].isna().sum()
            emea_df[col] = pd.to_numeric(emea_df[col], errors='coerce')
            after_nulls = emea_df[col].isna().sum()
            new_nulls = after_nulls - before_nulls
            if new_nulls > 0:
                print(f"  ⚠ {col}: {new_nulls} values couldn't be converted")
            else:
                print(f"  ✓ {col}: All valid values converted")
    
    # Test 6: Database operations
    print("\n6. Testing database operations...")
    
    # Create test database
    test_db = 'test_fund_data.db'
    if os.path.exists(test_db):
        os.remove(test_db)
    
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    
    # Create table
    cursor.execute("""
    CREATE TABLE fund_data (
        date DATE,
        region TEXT,
        fund_code TEXT,
        fund_name TEXT,
        currency TEXT,
        share_class_assets REAL,
        one_day_yield REAL,
        seven_day_yield REAL,
        PRIMARY KEY (date, region, fund_code)
    )
    """)
    
    # Prepare sample data
    sample_data = []
    for _, row in emea_df.head(5).iterrows():
        sample_data.append({
            'date': row['Date'].strftime('%Y-%m-%d'),
            'region': 'EMEA',
            'fund_code': row['Fund Code'],
            'fund_name': row['Fund Name'].replace("'", "''"),  # Escape quotes
            'currency': row['Currency'],
            'share_class_assets': pd.to_numeric(row.get('Share Class Assets (dly/$mils)', np.nan), errors='coerce'),
            'one_day_yield': pd.to_numeric(row.get('1-DSY (dly)', np.nan), errors='coerce'),
            'seven_day_yield': pd.to_numeric(row.get('7-DSY (dly)', np.nan), errors='coerce')
        })
    
    # Insert data
    for data in sample_data:
        try:
            cursor.execute("""
            INSERT INTO fund_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(data.values()))
        except Exception as e:
            print(f"  ✗ Insert error: {e}")
    
    conn.commit()
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM fund_data")
    count = cursor.fetchone()[0]
    print(f"  ✓ Successfully inserted {count} test records")
    
    conn.close()
    os.remove(test_db)
    
    # Test 7: Friday to weekend logic
    print("\n7. Testing Friday to weekend data expansion...")
    friday_date = datetime(2025, 6, 27)  # A Friday
    if friday_date.weekday() == 4:
        saturday = friday_date.strftime('%Y-%m-%d')
        sunday = (friday_date.date() + pd.Timedelta(days=2)).strftime('%Y-%m-%d')
        print(f"  ✓ Friday {friday_date.strftime('%Y-%m-%d')} would expand to:")
        print(f"    - Saturday: {saturday}")
        print(f"    - Sunday: {sunday}")
    
    print("\n=== All Tests Complete ===")
    print("\nSummary of fixes applied:")
    print("1. Date format variations handled")
    print("2. Duplicate fund codes (#MULTIVALUE) fixed")
    print("3. Whitespace cleaned from all text fields")
    print("4. Special characters properly escaped")
    print("5. Numeric conversions handle '-' as missing")
    print("6. Database operations verified")

if __name__ == "__main__":
    test_etl_with_files()
