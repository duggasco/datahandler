#!/usr/bin/env python3
"""
Test script to create an active workflow and verify it displays properly
"""

import requests
import time
import json

def test_active_workflow_display():
    """Test creating an active workflow and checking if it displays"""
    
    print("Testing Active Workflow Display")
    print("=" * 50)
    
    # Create a validation workflow
    print("\n1. Creating a validation workflow...")
    try:
        response = requests.post(
            'http://localhost:8080/api/workflow/validate',
            json={'mode': 'selective'},
            timeout=10
        )
        
        if response.status_code in [200, 202]:
            data = response.json()
            workflow_id = data.get('workflow_id')
            print(f"✅ Workflow created: {workflow_id}")
            
            # Wait a moment for it to start
            time.sleep(3)
            
            # Check workflow list
            print("\n2. Checking workflow list...")
            response = requests.get('http://localhost:8080/api/workflow/list')
            
            if response.status_code == 200:
                workflows = response.json()
                print(f"✅ Found {len(workflows)} total workflows")
                
                # Find running workflows
                running_workflows = [w for w in workflows if w['status'] == 'running']
                print(f"✅ Found {len(running_workflows)} running workflows")
                
                # Check our workflow
                our_workflow = None
                for wf in workflows:
                    if wf['id'] == workflow_id:
                        our_workflow = wf
                        break
                
                if our_workflow:
                    print(f"\n3. Our workflow details:")
                    print(f"   Status: {our_workflow['status']}")
                    print(f"   Type: {our_workflow['type']}")
                    print(f"   Started at: {our_workflow.get('started_at', 'None')}")
                    print(f"   Created at: {our_workflow.get('created_at', 'None')}")
                    print(f"   Output lines: {len(our_workflow.get('output', []))}")
                    
                    if our_workflow['status'] == 'running' and our_workflow.get('started_at'):
                        print("\n✅ Active workflow is properly configured with started_at timestamp!")
                    else:
                        print("\n⚠️  Workflow missing started_at or not in running status")
                else:
                    print("\n❌ Our workflow not found in list!")
            else:
                print(f"❌ Failed to get workflow list: {response.status_code}")
                
        else:
            print(f"❌ Failed to create workflow: {response.status_code} - {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Unable to connect to UI API. Make sure the UI container is running.")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    test_active_workflow_display()
    print("\n✨ Test complete!")