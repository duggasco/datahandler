#!/usr/bin/env python3
"""
Diagnostic script to identify why selective validation is updating every row
"""

import sys
import os
sys.path.append('/app')

from fund_etl_pipeline import FundDataETL
import pandas as pd
import sqlite3
from datetime import datetime

def diagnose_validation_issues():
    """Diagnose why all rows are being flagged as changed"""
    print("=== Validation Diagnostic Tool ===\n")
    
    etl = FundDataETL('/config/config.json')
    
    # Test with a small sample
    region = 'AMRS'  # or 'EMEA'
    
    print(f"1. Downloading lookback file for {region}...")
    lookback_df = etl.download_lookback_file(region)
    
    if lookback_df is None:
        print("Failed to download lookback file")
        return
    
    print(f"   Lookback file has {len(lookback_df)} records")
    
    # Get a sample date from the lookback file
    sample_date = lookback_df['Date'].iloc[0]
    print(f"\n2. Using sample date: {sample_date}")
    
    # Get data from database for this date
    conn = sqlite3.connect(etl.db_path)
    db_query = f"""
    SELECT * FROM fund_data 
    WHERE date = '{sample_date.strftime('%Y-%m-%d')}' 
    AND region = '{region}'
    LIMIT 5
    """
    db_data = pd.read_sql_query(db_query, conn)
    
    print(f"   Found {len(db_data)} records in database for this date (showing first 5)")
    
    # Get corresponding lookback data
    lookback_sample = lookback_df[lookback_df['Date'].dt.date == sample_date.date()].head(5)
    
    print("\n3. Detailed field-by-field comparison:")
    print("="*80)
    
    # Critical fields from config
    critical_fields = etl.config.get('validation', {}).get('critical_fields', 
        ['share_class_assets', 'portfolio_assets', 'one_day_yield', 'seven_day_yield'])
    
    field_mapping = {
        'share_class_assets': 'Share Class Assets (dly/$mils)',
        'portfolio_assets': 'Portfolio Assets (dly/$mils)',
        'one_day_yield': '1-DSY (dly)',
        'seven_day_yield': '7-DSY (dly)'
    }
    
    for i, db_row in db_data.iterrows():
        fund_code = db_row['fund_code']
        print(f"\nFund: {fund_code}")
        print("-" * 40)
        
        # Find corresponding lookback record
        lookback_row = lookback_sample[lookback_sample['Fund Code'] == fund_code]
        
        if len(lookback_row) == 0:
            print("  NOT FOUND in lookback file")
            continue
            
        lookback_row = lookback_row.iloc[0]
        
        # Compare each critical field
        for db_field, excel_field in field_mapping.items():
            if db_field in critical_fields:
                db_value = db_row[db_field]
                lookback_value = lookback_row[excel_field]
                
                print(f"\n  {db_field}:")
                print(f"    Database value: {db_value} (type: {type(db_value).__name__})")
                print(f"    Lookback value: {lookback_value} (type: {type(lookback_value).__name__})")
                
                # Convert for comparison
                if pd.notna(lookback_value) and lookback_value != '-':
                    lookback_numeric = pd.to_numeric(lookback_value, errors='coerce')
                else:
                    lookback_numeric = None
                
                print(f"    Lookback numeric: {lookback_numeric}")
                
                # Check if values are equal
                if pd.isna(db_value) and pd.isna(lookback_numeric):
                    print("    → Both NULL - NO CHANGE")
                elif pd.isna(db_value) != pd.isna(lookback_numeric):
                    print("    → NULL MISMATCH - WOULD UPDATE")
                elif pd.notna(db_value) and pd.notna(lookback_numeric):
                    # Compare with precision
                    if isinstance(db_value, (int, float)):
                        db_float = float(db_value)
                        diff = abs(db_float - lookback_numeric)
                        
                        if diff < 1e-10:
                            print(f"    → Values equal (diff: {diff:.2e}) - NO CHANGE")
                        else:
                            if db_float != 0:
                                pct_change = abs((lookback_numeric - db_float) / db_float * 100)
                                print(f"    → Values differ by {pct_change:.2f}% - ", end="")
                                if pct_change > 5.0:
                                    print("WOULD UPDATE")
                                else:
                                    print("NO CHANGE (below 5% threshold)")
                            else:
                                print(f"    → DB zero, lookback {lookback_numeric} - WOULD UPDATE")
    
    conn.close()
    
    print("\n" + "="*80)
    print("\n4. Configuration Summary:")
    print(f"   Change threshold: {etl.config.get('validation', {}).get('change_threshold_percent', 5.0)}%")
    print(f"   Critical fields: {critical_fields}")
    print(f"   Update mode: {etl.config.get('validation', {}).get('update_mode', 'selective')}")
    
    # Check for common issues
    print("\n5. Common Issue Checks:")
    
    # Check if lookback file has different precision
    sample_assets = lookback_sample['Share Class Assets (dly/$mils)'].dropna().head()
    print(f"\n   Sample asset values from lookback:")
    for val in sample_assets:
        print(f"     {val} (has decimal: {isinstance(val, float) and val != int(val)})")
    
    # Check date formats
    print(f"\n   Lookback date type: {type(lookback_df['Date'].iloc[0])}")
    print(f"   Database date format: YYYY-MM-DD string")
    
    print("\n=== End Diagnostic ===")

if __name__ == "__main__":
    diagnose_validation_issues()
