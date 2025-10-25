#!/usr/bin/env python3
"""
Simple connection test for Kafka and MongoDB
Run this first to verify your credentials work
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_imports():
    """Test if all required packages are installed"""
    print("Testing imports...")
    try:
        import confluent_kafka
        import pymongo
        import pandas
        print("✓ All packages imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        print("Please run: pip install -r requirements.txt")
        return False

def test_env_variables():
    """Test if environment variables are set"""
    print("\nTesting environment variables...")
    required_vars = [
        'KAFKA_BOOTSTRAP_SERVERS',
        'KAFKA_API_KEY', 
        'KAFKA_API_SECRET',
        'MONGODB_URI',
        'KAFKA_TOPIC'
    ]
    
    missing = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing.append(var)
        else:
            # Mask sensitive values
            if 'SECRET' in var or 'URI' in var:
                masked = value[:10] + "..." + value[-5:] if len(value) > 15 else "***"
                print(f"✓ {var}: {masked}")
            else:
                print(f"✓ {var}: {value}")
    
    if missing:
        print(f"✗ Missing variables: {missing}")
        return False
    
    print("✓ All environment variables set")
    return True

def test_mongodb_connection():
    """Test MongoDB connection"""
    print("\nTesting MongoDB connection...")
    try:
        from pymongo import MongoClient
        
        uri = os.getenv('MONGODB_URI')
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # Test connection
        client.admin.command('ping')
        print("✓ MongoDB connection successful")
        
        # Test database access
        db_name = os.getenv('MONGODB_DATABASE', 'supply_chain_analytics')
        db = client[db_name]
        collections = db.list_collection_names()
        print(f"✓ Database '{db_name}' accessible, collections: {len(collections)}")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"✗ MongoDB connection failed: {e}")
        return False

def test_kafka_connection():
    """Test Kafka connection"""
    print("\nTesting Kafka connection...")
    try:
        from confluent_kafka import Producer, Consumer
        
        config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': os.getenv('KAFKA_API_KEY'),
            'sasl.password': os.getenv('KAFKA_API_SECRET'),
        }
        
        # Test producer
        producer = Producer(config)
        print("✓ Kafka producer created successfully")
        
        # Test consumer
        consumer_config = config.copy()
        consumer_config.update({
            'group.id': 'test-group',
            'auto.offset.reset': 'earliest'
        })
        consumer = Consumer(consumer_config)
        print("✓ Kafka consumer created successfully")
        
        # Test topic subscription
        topic = os.getenv('KAFKA_TOPIC', 'supply-chain-events')
        consumer.subscribe([topic])
        print(f"✓ Subscribed to topic: {topic}")
        
        consumer.close()
        return True
        
    except Exception as e:
        print(f"✗ Kafka connection failed: {e}")
        return False

def test_data_file():
    """Test if data file exists and is readable"""
    print("\nTesting data file...")
    try:
        import pandas as pd
        
        file_path = 'data/cleaned_supply_chain_data.csv'
        if not os.path.exists(file_path):
            print(f"✗ Data file not found: {file_path}")
            return False
        
        df = pd.read_csv(file_path)
        print(f"✓ Data file loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Show first few column names
        cols = list(df.columns)[:5]
        print(f"✓ Sample columns: {cols}")
        
        return True
        
    except Exception as e:
        print(f"✗ Data file test failed: {e}")
        return False

def main():
    """Run all connection tests"""
    print("=== Supply Chain Analytics Connection Test ===\n")
    
    tests = [
        ("Package Imports", test_imports),
        ("Environment Variables", test_env_variables),
        ("Data File", test_data_file),
        ("MongoDB Connection", test_mongodb_connection),
        ("Kafka Connection", test_kafka_connection)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    print("\n=== Test Results ===")
    all_passed = True
    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 All tests passed! Ready to start streaming.")
        print("\nNext steps:")
        print("1. Run: python kafka_producer.py")
        print("2. In another terminal: python kafka_consumer.py")
        print("3. Or run complete test: python test_streaming.py")
    else:
        print("\n❌ Some tests failed. Please fix the issues above.")
        sys.exit(1)

if __name__ == "__main__":
    main()