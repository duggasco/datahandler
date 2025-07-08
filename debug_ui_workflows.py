#!/usr/bin/env python3
"""
Debug workflow tracking in UI container
"""

import sys
import os

# Test if we can import the module
try:
    from workflow_db_tracker import DatabaseWorkflowTracker
    print("✅ Successfully imported DatabaseWorkflowTracker")
except Exception as e:
    print(f"❌ Failed to import DatabaseWorkflowTracker: {e}")
    sys.exit(1)

# Test database connection
try:
    db_path = '/data/fund_data.db'
    print(f"\nTesting database at: {db_path}")
    print(f"Database exists: {os.path.exists(db_path)}")
    
    # Create tracker instance
    tracker = DatabaseWorkflowTracker(db_path)
    print("✅ Created DatabaseWorkflowTracker instance")
    
    # Get all workflows
    workflows = tracker.get_all_workflows()
    print(f"\n📊 Found {len(workflows)} workflows")
    
    for wf in workflows[:3]:
        print(f"  - {wf['id'][:8]}... | {wf['type']} | {wf['status']}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()