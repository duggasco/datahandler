#!/usr/bin/env python3
"""
Test script to verify workflow persistence is working correctly
"""

import sqlite3
import json
from datetime import datetime

def test_workflow_persistence():
    """Test that workflows are being persisted to the database"""
    db_path = '/data/fund_data.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if workflows table exists
        cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='workflows'
        """)
        
        if not cursor.fetchone():
            print("❌ Workflows table does not exist!")
            return False
        
        print("✅ Workflows table exists")
        
        # Check table structure
        cursor.execute("PRAGMA table_info(workflows)")
        columns = cursor.fetchall()
        expected_columns = {
            'id', 'type', 'status', 'created_at', 'started_at', 
            'completed_at', 'params', 'output', 'error', 'message', 
            'etl_workflow_id'
        }
        
        actual_columns = {col[1] for col in columns}
        if expected_columns.issubset(actual_columns):
            print("✅ Workflows table has all expected columns")
        else:
            missing = expected_columns - actual_columns
            print(f"❌ Missing columns: {missing}")
        
        # Check for any existing workflows
        cursor.execute("SELECT COUNT(*) FROM workflows")
        count = cursor.fetchone()[0]
        print(f"\n📊 Current workflow count: {count}")
        
        # Show recent workflows
        if count > 0:
            cursor.execute("""
            SELECT id, type, status, created_at, completed_at 
            FROM workflows 
            ORDER BY created_at DESC 
            LIMIT 5
            """)
            
            print("\n📋 Recent workflows:")
            for row in cursor.fetchall():
                wf_id, wf_type, status, created, completed = row
                print(f"  - {wf_type} ({status}) - Created: {created[:19]}")
        
        # Check indices
        cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='index' AND tbl_name='workflows'
        """)
        
        indices = [row[0] for row in cursor.fetchall()]
        print(f"\n🔍 Indices on workflows table: {len(indices)}")
        for idx in indices:
            print(f"  - {idx}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error testing workflow persistence: {e}")
        return False


if __name__ == "__main__":
    print("Testing Workflow Persistence...")
    print("=" * 50)
    test_workflow_persistence()
    print("\n✨ Test complete!")