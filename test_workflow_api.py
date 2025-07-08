#!/usr/bin/env python3
"""
Test the workflow API to ensure persistence is working
"""

import requests
import time
import sqlite3
import json

def test_workflow_api():
    """Test creating and retrieving workflows via API"""
    
    # Test validation workflow through UI API
    print("1. Testing validation workflow creation...")
    try:
        response = requests.post(
            'http://localhost:8080/api/workflow/validate',
            json={'mode': 'selective'},
            timeout=10
        )
        
        if response.status_code in [200, 202]:
            data = response.json()
            workflow_id = data.get('workflow_id')
            print(f"✅ Validation workflow created: {workflow_id}")
            
            # Wait a moment for it to be persisted
            time.sleep(2)
            
            # Check if it's in the database
            conn = sqlite3.connect('/data/fund_data.db')
            cursor = conn.cursor()
            cursor.execute("SELECT type, status FROM workflows WHERE id = ?", (workflow_id,))
            row = cursor.fetchone()
            
            if row:
                print(f"✅ Workflow found in database: type={row[0]}, status={row[1]}")
            else:
                print("❌ Workflow not found in database!")
            
            conn.close()
            
            # Check workflow list endpoint
            print("\n2. Testing workflow list endpoint...")
            response = requests.get('http://localhost:8080/api/workflow/list')
            if response.status_code == 200:
                workflows = response.json()
                print(f"✅ Found {len(workflows)} workflows in list")
                
                # Find our workflow
                found = False
                for wf in workflows:
                    if wf['id'] == workflow_id:
                        found = True
                        print(f"✅ Our workflow is in the list: {wf['type']} - {wf['status']}")
                        break
                
                if not found:
                    print("❌ Our workflow not found in list!")
            else:
                print(f"❌ Failed to get workflow list: {response.status_code}")
        else:
            print(f"❌ Failed to create workflow: {response.status_code} - {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Unable to connect to UI API. Make sure the UI container is running.")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    print("Testing Workflow API Persistence...")
    print("=" * 50)
    test_workflow_api()
    print("\n✨ API test complete!")