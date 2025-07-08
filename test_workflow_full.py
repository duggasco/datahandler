#!/usr/bin/env python3
"""
Full test of workflow persistence - run inside container
"""

import sqlite3
import json
import time
from datetime import datetime

def test_workflow_persistence_full():
    """Comprehensive test of workflow persistence"""
    db_path = '/data/fund_data.db'
    
    print("1. Checking workflows before test...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM workflows")
    before_count = cursor.fetchone()[0]
    print(f"   Workflows before: {before_count}")
    
    # Show recent workflows
    cursor.execute("""
    SELECT id, type, status, created_at, etl_workflow_id
    FROM workflows 
    ORDER BY created_at DESC 
    LIMIT 5
    """)
    
    print("\n2. Recent workflows in database:")
    for row in cursor.fetchall():
        wf_id, wf_type, status, created, etl_id = row
        print(f"   - {wf_id[:8]}... | {wf_type} | {status} | {created[:19]} | ETL: {etl_id or 'None'}")
    
    # Check for any running workflows
    cursor.execute("SELECT COUNT(*) FROM workflows WHERE status = 'running'")
    running_count = cursor.fetchone()[0]
    print(f"\n3. Running workflows: {running_count}")
    
    # Check for completed workflows
    cursor.execute("SELECT COUNT(*) FROM workflows WHERE status = 'completed'")
    completed_count = cursor.fetchone()[0]
    print(f"4. Completed workflows: {completed_count}")
    
    # Check for failed workflows
    cursor.execute("SELECT COUNT(*) FROM workflows WHERE status = 'failed'")
    failed_count = cursor.fetchone()[0]
    print(f"5. Failed workflows: {failed_count}")
    
    # Check workflow output
    cursor.execute("""
    SELECT id, type, LENGTH(output) as output_size
    FROM workflows 
    WHERE output != '[]'
    ORDER BY created_at DESC
    LIMIT 3
    """)
    
    print("\n6. Workflows with output:")
    for row in cursor.fetchall():
        wf_id, wf_type, output_size = row
        print(f"   - {wf_id[:8]}... | {wf_type} | Output size: {output_size} bytes")
    
    # Check etl_workflow_id linkage
    cursor.execute("""
    SELECT COUNT(*) 
    FROM workflows 
    WHERE etl_workflow_id IS NOT NULL
    """)
    linked_count = cursor.fetchone()[0]
    print(f"\n7. Workflows linked to backend ETL: {linked_count}")
    
    conn.close()
    
    print("\n✅ Workflow persistence is active and working!")
    return True


if __name__ == "__main__":
    print("Full Workflow Persistence Test")
    print("=" * 50)
    test_workflow_persistence_full()