#!/usr/bin/env python3
"""
Test script to verify BigQuery tables and data for the dashboard
"""

import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="configs/.env")

def test_bigquery_connection():
    """Test BigQuery connection and table access"""
    try:
        client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
        project_id = os.getenv("BIGQUERY_PROJECT")
        dataset = os.getenv("BIGQUERY_DATASET")
        
        print(f"🔍 Testing BigQuery connection...")
        print(f"Project: {project_id}")
        print(f"Dataset: {dataset}")
        
        # Test 1: Check if shipment_metrics table exists
        print("\n📊 Testing shipment_metrics table...")
        try:
            query = f"""
            SELECT COUNT(*) as row_count
            FROM `{project_id}.{dataset}.shipment_metrics`
            """
            result = client.query(query).to_dataframe()
            print(f"✅ shipment_metrics table found with {result.iloc[0]['row_count']} rows")
        except Exception as e:
            print(f"❌ shipment_metrics table error: {str(e)}")
        
        # Test 2: Check table schema
        print("\n🔍 Checking shipment_metrics schema...")
        try:
            query = f"""
            SELECT column_name, data_type
            FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'shipment_metrics'
            ORDER BY ordinal_position
            LIMIT 10
            """
            schema_df = client.query(query).to_dataframe()
            print("✅ First 10 columns:")
            for _, row in schema_df.iterrows():
                print(f"  - {row['column_name']}: {row['data_type']}")
        except Exception as e:
            print(f"❌ Schema check error: {str(e)}")
        
        # Test 3: Check if shipment_performance_metrics view exists
        print("\n📈 Testing shipment_performance_metrics view...")
        try:
            query = f"""
            SELECT *
            FROM `{project_id}.{dataset}.shipment_performance_metrics`
            """
            result = client.query(query).to_dataframe()
            print(f"✅ shipment_performance_metrics view found")
            print("Columns:", list(result.columns))
            if not result.empty:
                print("Sample data:", result.iloc[0].to_dict())
        except Exception as e:
            print(f"❌ shipment_performance_metrics view error: {str(e)}")
        
        # Test 4: Sample data from shipment_metrics
        print("\n📋 Sample data from shipment_metrics...")
        try:
            query = f"""
            SELECT 
                _id,
                event_id,
                timestamp,
                label_delay_hours,
                gps_latitude,
                gps_longitude,
                risk_classification
            FROM `{project_id}.{dataset}.shipment_metrics`
            ORDER BY timestamp DESC
            LIMIT 3
            """
            sample_df = client.query(query).to_dataframe()
            print(f"✅ Sample data retrieved ({len(sample_df)} rows):")
            for _, row in sample_df.iterrows():
                print(f"  ID: {row['_id']}, Delay: {row['label_delay_hours']:.2f}h, Risk: {row['risk_classification']}")
        except Exception as e:
            print(f"❌ Sample data error: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"❌ BigQuery connection failed: {str(e)}")
        return False

def test_ml_model():
    """Test ML model availability"""
    try:
        client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
        project_id = os.getenv("BIGQUERY_PROJECT")
        dataset = os.getenv("BIGQUERY_DATASET")
        model_name = os.getenv("BQ_MODEL", "eta_delay_dnn")
        
        print(f"\n🤖 Testing ML model: {model_name}")
        
        # Check if model exists
        query = f"""
        SELECT model_name, model_type, creation_time
        FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.ML_MODELS`
        WHERE model_name = '{model_name}'
        """
        result = client.query(query).to_dataframe()
        
        if result.empty:
            print(f"❌ ML model '{model_name}' not found")
            return False
        else:
            print(f"✅ ML model found: {result.iloc[0]['model_type']}")
            print(f"   Created: {result.iloc[0]['creation_time']}")
            return True
            
    except Exception as e:
        print(f"❌ ML model test failed: {str(e)}")
        return False

def main():
    print("🧪 Dashboard Data Test")
    print("=" * 50)
    
    # Test BigQuery connection and tables
    bq_success = test_bigquery_connection()
    
    # Test ML model
    ml_success = test_ml_model()
    
    print("\n" + "=" * 50)
    if bq_success and ml_success:
        print("✅ All tests passed! Dashboard should work correctly.")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        if not bq_success:
            print("   - Fix BigQuery table access issues")
        if not ml_success:
            print("   - Ensure ML model is trained and available")

if __name__ == "__main__":
    main()