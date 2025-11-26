"""
Test script to verify connections work before running Airflow
Run this first: python test_connection.py
"""

import sys
import os

print("=" * 60)
print("Testing Supply Chain Pipeline Connections")
print("=" * 60)

# Test 1: Google Credentials
print("\n1. Testing Google Credentials...")
creds_path = "include/google-credentials.json"
if os.path.exists(creds_path):
    print(f"   ✅ Found credentials at: {creds_path}")
    import json
    with open(creds_path) as f:
        creds = json.load(f)
        print(f"   ✅ Service Account: {creds.get('client_email', 'N/A')}")
else:
    print(f"   ❌ Credentials not found at: {creds_path}")
    print(f"   Run: cp ../configs/google-credentials.json {creds_path}")
    sys.exit(1)

# Test 2: Google Sheets Access
print("\n2. Testing Google Sheets Access...")
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    
    scope = ['https://spreadsheets.google.com/feeds', 
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    print("   ✅ Google Sheets authentication successful")
    
    # Try to access a sheet (you need to provide Sheet ID)
    sheet_id = input("\n   Enter your Google Sheet ID (or press Enter to skip): ").strip()
    if sheet_id:
        try:
            sheet = client.open_by_key(sheet_id).sheet1
            records = sheet.get_all_records()
            print(f"   ✅ Successfully accessed sheet with {len(records)} rows")
        except Exception as e:
            print(f"   ❌ Could not access sheet: {e}")
            print(f"   Make sure sheet is shared with: {creds.client_email}")
    
except ImportError as e:
    print(f"   ❌ Missing package: {e}")
    print("   Run: pip install gspread oauth2client")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Test 3: MongoDB Connection
print("\n3. Testing MongoDB Connection...")
try:
    from pymongo import MongoClient
    
    mongo_uri = "mongodb+srv://harishnandhan02_db_user:harish03@cluster-harish.dgbpxws.mongodb.net/?retryWrites=true&w=majority&appName=cluster-harish"
    
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.server_info()  # Force connection
    
    db = client['supply_chain_analytics']
    collection = db['scheduler_table']
    count = collection.count_documents({})
    
    print(f"   ✅ MongoDB connected successfully")
    print(f"   ✅ Collection 'scheduler_table' has {count} documents")
    
except ImportError:
    print("   ❌ pymongo not installed")
    print("   Run: pip install pymongo")
except Exception as e:
    print(f"   ❌ MongoDB connection failed: {e}")

# Test 4: BigQuery Connection
print("\n4. Testing BigQuery Connection...")
try:
    from google.cloud import bigquery
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
    client = bigquery.Client(project='supply-chain-project-476405')
    
    # Try to list datasets
    datasets = list(client.list_datasets())
    print(f"   ✅ BigQuery connected successfully")
    print(f"   ✅ Found {len(datasets)} datasets")
    
    # Check if our dataset exists
    dataset_id = 'supply_chain'
    try:
        dataset = client.get_dataset(dataset_id)
        print(f"   ✅ Dataset '{dataset_id}' exists")
    except:
        print(f"   ⚠️  Dataset '{dataset_id}' not found (will be created)")
    
except ImportError:
    print("   ❌ google-cloud-bigquery not installed")
    print("   Run: pip install google-cloud-bigquery")
except Exception as e:
    print(f"   ❌ BigQuery connection failed: {e}")

print("\n" + "=" * 60)
print("Connection Test Complete!")
print("=" * 60)
print("\nIf all tests passed, you can proceed with:")
print("  astro dev start")
