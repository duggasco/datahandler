#!/usr/bin/env python3
"""
Test SAP OpenDocument connectivity and authentication
"""

import requests
from requests.auth import HTTPBasicAuth
import json
from urllib.parse import urlparse
import sys

def test_sap_connectivity():
    """Test connectivity to SAP OpenDocument URLs"""
    
    # SAP OpenDocument URLs
    urls = {
        'AMRS': 'https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AYscKsmnmVFMgwa4u8GO5GU&sOutputFormat=E',
        'EMEA': 'https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AXFSzkEFSQpOrrU9_35AhpQ&sOutputFormat=E'
    }
    
    print("=== SAP OpenDocument Connectivity Test ===\n")
    
    # Parse URLs
    for region, url in urls.items():
        parsed = urlparse(url)
        print(f"{region} URL Details:")
        print(f"  Host: {parsed.netloc}")
        print(f"  Path: {parsed.path}")
        print(f"  Document ID: {url.split('iDocID=')[1].split('&')[0]}")
        print()
    
    # Test without authentication first
    print("Testing connectivity without authentication...\n")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    for region, url in urls.items():
        print(f"Testing {region}...")
        try:
            # Try HEAD request first
            response = session.head(url, timeout=10, allow_redirects=True)
            print(f"  Status Code: {response.status_code}")
            print(f"  Headers: {dict(response.headers)}")
            
            if response.status_code == 401:
                print("  → Authentication required (401 Unauthorized)")
            elif response.status_code == 403:
                print("  → Access forbidden (403)")
            elif response.status_code == 302 or response.status_code == 301:
                print(f"  → Redirect to: {response.headers.get('Location', 'Unknown')}")
            elif response.status_code == 200:
                print("  → Connection successful!")
                print(f"  → Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
            else:
                print(f"  → Unexpected response: {response.status_code}")
                
        except requests.exceptions.SSLError:
            print("  → SSL Error - trying without SSL verification...")
            try:
                response = session.head(url, timeout=10, verify=False)
                print(f"  → Status without SSL verify: {response.status_code}")
                print("  ⚠️  WARNING: SSL certificate verification failed")
            except Exception as e:
                print(f"  → Failed even without SSL: {str(e)}")
                
        except requests.exceptions.Timeout:
            print("  → Connection timeout (server may be slow)")
        except requests.exceptions.ConnectionError as e:
            print(f"  → Connection error: {str(e)}")
        except Exception as e:
            print(f"  → Unexpected error: {type(e).__name__}: {str(e)}")
        
        print()
    
    # Test with authentication
    print("\nTo test with authentication, create a config file 'sap_auth.json':")
    print("(Default credentials are provided)")
    print(json.dumps({"username": "sduggan", "password": "sduggan"}, indent=2))
    
    try:
        with open('sap_auth.json', 'r') as f:
            auth_config = json.load(f)
            
        print("\nTesting with authentication...")
        auth = HTTPBasicAuth(auth_config['username'], auth_config['password'])
        
        for region, url in urls.items():
            print(f"\nTesting {region} with auth...")
            try:
                response = session.get(
                    url, 
                    auth=auth, 
                    timeout=30,
                    stream=True,  # Don't download entire file
                    headers={'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}
                )
                
                print(f"  Status Code: {response.status_code}")
                
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '')
                    content_length = response.headers.get('Content-Length', 'Unknown')
                    
                    print(f"  Content-Type: {content_type}")
                    print(f"  Content-Length: {content_length}")
                    
                    if 'excel' in content_type or 'spreadsheet' in content_type:
                        print("  ✓ Successfully authenticated and file is available!")
                    elif 'text/html' in content_type:
                        print("  ⚠️  Received HTML - may be login page")
                        # Read first 1000 bytes to check
                        content_sample = response.content[:1000].decode('utf-8', errors='ignore')
                        if 'login' in content_sample.lower():
                            print("  → Appears to be a login page")
                else:
                    print(f"  → Authentication may have failed: {response.status_code}")
                    
            except Exception as e:
                print(f"  → Error: {type(e).__name__}: {str(e)}")
                
    except FileNotFoundError:
        print("\nNo authentication config found. Skipping authenticated tests.")
    except json.JSONDecodeError:
        print("\nInvalid JSON in sap_auth.json")
    
    session.close()
    
    print("\n=== Connectivity Test Complete ===")
    print("\nRecommendations:")
    print("1. If you see 401 errors, authentication is required")
    print("2. If you see SSL errors, you may need to set verify_ssl=False in config")
    print("3. If you see timeouts, increase the timeout value")
    print("4. Check with your SAP admin for correct credentials and permissions")


def test_download_small_sample():
    """Try to download just the first few KB to test"""
    print("\n=== Testing Partial Download ===\n")
    
    url = 'https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AYscKsmnmVFMgwa4u8GO5GU&sOutputFormat=E'
    
    try:
        with open('sap_auth.json', 'r') as f:
            auth_config = json.load(f)
            
        auth = HTTPBasicAuth(auth_config['username'], auth_config['password'])
        
        # Try to download first 10KB
        headers = {
            'Range': 'bytes=0-10240',
            'User-Agent': 'Mozilla/5.0'
        }
        
        response = requests.get(url, auth=auth, headers=headers, timeout=30, verify=False)
        
        if response.status_code in [200, 206]:  # 206 is partial content
            print(f"Downloaded {len(response.content)} bytes")
            
            # Check if it's an Excel file (starts with PK)
            if response.content[:2] == b'PK':
                print("✓ File appears to be a valid Excel/ZIP file")
                with open('test_sample.xlsx', 'wb') as f:
                    f.write(response.content)
                print("Sample saved as test_sample.xlsx")
            else:
                print("File doesn't appear to be Excel format")
                print(f"First 50 bytes: {response.content[:50]}")
                
    except Exception as e:
        print(f"Download test failed: {e}")


if __name__ == "__main__":
    test_sap_connectivity()
    
    # Only test download if auth config exists
    try:
        with open('sap_auth.json', 'r'):
            test_download_small_sample()
    except:
        pass
