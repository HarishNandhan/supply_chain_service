from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import json
import logging

# Default arguments
default_args = {
    'owner': 'supply-chain',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'supply_chain_pipeline',
    default_args=default_args,
    description='Supply Chain Data Pipeline: Google Sheets → MongoDB → dbt → BigQuery → ML',
    schedule_interval=None,  # Manual trigger from Streamlit
    catchup=False,
    tags=['supply-chain', 'ml', 'etl'],
)

def extract_from_google_sheets(**context):
    """Extract ONE row from Google Sheets based on last processed row"""
    logging.info("Starting Google Sheets extraction (single row)...")
    
    # Get configuration
    sheet_id = Variable.get("google_sheet_id")
    
    # Get last processed row index (stored in Airflow Variable)
    try:
        last_row_index = int(Variable.get("last_processed_row_index", "0"))
    except:
        last_row_index = 0
    
    logging.info(f"Last processed row index: {last_row_index}")
    
    # Setup Google Sheets connection
    import os
    if os.path.exists("/usr/local/airflow/include/google-credentials.json"):
        creds_path = "/usr/local/airflow/include/google-credentials.json"
    else:
        creds_path = "/usr/local/airflow/configs/google-credentials.json"
    
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    # Open the sheet
    sheet = client.open_by_key(sheet_id).sheet1
    
    # Get all records
    all_records = sheet.get_all_records()
    
    if not all_records:
        logging.warning("No records found in Google Sheets")
        return 0
    
    # Get the next row to process
    next_row_index = last_row_index
    
    if next_row_index >= len(all_records):
        logging.warning(f"All rows processed! Resetting to row 0")
        next_row_index = 0
    
    # Extract single row
    single_record = all_records[next_row_index]
    
    logging.info(f"✓ Extracted row {next_row_index + 1} of {len(all_records)}")
    logging.info(f"Record keys: {list(single_record.keys())}")
    logging.info(f"Record values: {single_record}")
    
    # Update last processed row index for next run
    Variable.set("last_processed_row_index", str(next_row_index + 1))
    
    # Push single record to XCom
    context['task_instance'].xcom_push(key='extracted_data', value=[single_record])
    context['task_instance'].xcom_push(key='row_index', value=next_row_index)
    
    return 1

def load_to_mongodb_via_kafka(**context):
    """
    Send data to Kafka AND directly insert into MongoDB (no external consumer required).
    """
    logging.info("Starting Confluent Kafka producer...")

    from confluent_kafka import Producer
    import json
    import time as time_module

    # Get extracted data from XCom
    ti = context['task_instance']
    records = ti.xcom_pull(task_ids='extract_from_google_sheets', key='extracted_data')

    if not records:
        logging.warning("No data to send")
        return 0

    # Confluent Kafka configuration (same as kafka_producer.py)
    config = {
        'bootstrap.servers': Variable.get("kafka_bootstrap_servers"),
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': Variable.get("kafka_api_key"),
        'sasl.password': Variable.get("kafka_api_secret"),
        'client.id': 'airflow-supply-chain-producer',
        'socket.timeout.ms': 10000,
        'message.timeout.ms': 10000
    }

    topic = Variable.get("kafka_topic", "supply_chain")
    
    try:
        producer = Producer(config)
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        logging.warning("Continuing without Kafka - will only insert to MongoDB")
        producer = None

    def delivery_report(err, msg):
        if err is not None:
            logging.error(f'Message delivery failed: {err}')
        else:
            logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    logging.info(f"Sending {len(records)} records to Kafka topic: {topic}")

    # ✅ Set up Mongo so we can write directly
    mongo_hook = MongoHook(conn_id='mongodb_default')
    mongo_client = mongo_hook.get_conn()
    db = mongo_client[Variable.get("mongodb_database", "supply_chain_analytics")]
    collection = db[Variable.get("mongodb_collection", "scheduler_table")]

    sent_count = 0
    inserted_count = 0

    for record in records:
        try:
            # Transform to nested structure - map Google Sheet columns to nested format
            # Generate clean event_id from row index
            row_index = context['task_instance'].xcom_pull(task_ids='extract_from_google_sheets', key='row_index')
            event_id = f"evt_row_{row_index}_{int(time_module.time() * 1000)}"
            
            message = {
                'event_id': event_id,
                'timestamp': record.get('timestamp', datetime.utcnow().isoformat()),
                'vehicle_data': {
                    'gps_latitude': float(record.get('vehicle_gps_latitude', 0)),
                    'gps_longitude': float(record.get('vehicle_gps_longitude', 0)),
                    'eta_variation_hours': float(record.get('eta_variation_hours', 0))
                },
                'operational_metrics': {
                    'traffic_congestion_level': float(record.get('traffic_congestion_level', 0)),
                    'loading_unloading_time': float(record.get('loading_unloading_time', 0)),
                    'handling_equipment_availability': float(record.get('handling_equipment_availability', 0)),
                    'order_fulfillment_status': float(record.get('order_fulfillment_status', 0))
                },
                'external_factors': {
                    'weather_condition_severity': float(record.get('weather_condition_severity', 0)),
                    'port_congestion_level': float(record.get('port_congestion_level', 0)),
                    'shipping_costs': float(record.get('shipping_costs', 0))
                },
                'performance_indicators': {
                    'lead_time_days': float(record.get('lead_time_days', 0)),
                    'disruption_likelihood_score': float(record.get('disruption_likelihood_score', 0)),
                    'delay_probability': float(record.get('delay_probability', 0)),
                    'risk_classification': float(record.get('risk_classification', 0)),
                    'delivery_time_deviation': float(record.get('delivery_time_deviation', 0))
                },
                'temporal_features': {
                    'hour': int(record.get('hour', 0)),
                    'day': int(record.get('day', 0)),
                    'month': int(record.get('month', 0)),
                    'weekday': int(record.get('weekday', 0))
                },
                'ingestion_timestamp': datetime.utcnow().isoformat(),
                'airflow_run_id': context['run_id'],
                # 🔑 status fields for downstream pipeline
                'processing_status': 'success',
                'processed_timestamp': datetime.utcnow().isoformat(),
            }

            # 1) ✅ Send to Kafka (optional, for streaming / observability)
            if producer:
                try:
                    producer.produce(
                        topic=topic,
                        key=message['event_id'],
                        value=json.dumps(message),
                        callback=delivery_report
                    )
                    sent_count += 1
                    producer.poll(0)
                except Exception as kafka_err:
                    logging.warning(f"Kafka send failed: {kafka_err}")

            # 2) ✅ Directly insert into MongoDB for this pipeline
            collection.insert_one(message)
            inserted_count += 1

        except Exception as e:
            logging.error(f"Failed to send/insert record: {e}")

    # Flush with timeout to prevent hanging
    if producer:
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logging.warning(f"⚠️ {remaining} messages were not delivered to Kafka (timeout)")
        logging.info(f"✓ Successfully sent {sent_count} records to Kafka")
    else:
        logging.info("⚠️ Kafka producer was not available - skipped Kafka sending")
    logging.info(f"✓ Successfully inserted {inserted_count} records into MongoDB scheduler_table")

    return inserted_count

def transform_and_load_to_bigquery(**context):
    """
    Transform ONLY the current run's data from shipments_raw into test_table_airflow
    with the SAME schema as shipment_metrics (feature-engineered).
    """
    logging.info("Starting feature transformation for THIS RUN only...")

    # 1. Read the IDs that were just loaded to shipments_raw
    ti = context['task_instance']
    loaded_ids = ti.xcom_pull(task_ids='load_to_bigquery', key='loaded_ids')

    if not loaded_ids:
        logging.warning("No loaded_ids found from XCom. Nothing to transform.")
        return 0

    logging.info(f"Transforming {len(loaded_ids)} records for this run")

    # Build IN list for BigQuery (_id is stored as STRING in BQ)
    id_list_sql = ", ".join([f"'{i}'" for i in loaded_ids])

    # 2. BigQuery client
    bq_hook = BigQueryHook(gcp_conn_id='bigquery_default', use_legacy_sql=False)
    client = bq_hook.get_client()

    project_id = Variable.get("bigquery_project")
    dataset_id = Variable.get("bigquery_dataset")

    # 3. Create stg_shipments view (flattened raw data just for this run)
    logging.info("Step 1: Creating stg_shipments view for this run...")

    stg_query = f"""
    CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.stg_shipments` AS
    SELECT
        _id,
        event_id,
        timestamp,
        ingestion_timestamp,
        processed_timestamp,
        processing_status,
        -- base numeric features from raw
        vehicle_data_gps_latitude AS gps_latitude,
        vehicle_data_gps_longitude AS gps_longitude,
        vehicle_data_eta_variation_hours AS eta_variation_hours,
        operational_metrics_traffic_congestion_level AS traffic_congestion_level,
        operational_metrics_loading_unloading_time AS loading_unloading_time,
        operational_metrics_handling_equipment_availability AS handling_equipment_availability,
        operational_metrics_order_fulfillment_status AS order_fulfillment_status,
        external_factors_weather_condition_severity AS weather_condition_severity,
        external_factors_port_congestion_level AS port_congestion_level,
        external_factors_shipping_costs AS shipping_costs,
        performance_indicators_lead_time_days AS lead_time_days,
        performance_indicators_disruption_likelihood_score AS disruption_likelihood_score,
        performance_indicators_delay_probability AS delay_probability,
        performance_indicators_risk_classification AS risk_classification,
        performance_indicators_delivery_time_deviation AS delivery_time_deviation,
        temporal_features_hour AS hour,
        temporal_features_day AS day,
        temporal_features_month AS month,
        temporal_features_weekday AS weekday
    FROM `{project_id}.{dataset_id}.shipments_raw`
    WHERE _id IN ({id_list_sql})
    """

    client.query(stg_query).result()
    logging.info("✓ stg_shipments view created")

    # 4. Create test_table_airflow using the existing shipment_metrics view
    #    This ensures EXACT same schema and transformations
    logging.info("Step 2: Creating test_table_airflow from shipment_metrics view...")

    transform_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.test_table_airflow` AS
    SELECT * FROM `{project_id}.{dataset_id}.shipment_metrics`
    WHERE _id IN ({id_list_sql})
    """

    client.query(transform_query).result()
    logging.info("✓ test_table_airflow created with EXACT shipment_metrics schema and transformations")

    # Optional sanity check
    debug_query = f"SELECT COUNT(*) AS cnt FROM `{project_id}.{dataset_id}.test_table_airflow`"
    cnt = list(client.query(debug_query).result())[0].cnt
    logging.info(f"test_table_airflow row count for this run: {cnt}")

    # 5. Update MongoDB status -> transformed
    mongo_hook = MongoHook(conn_id='mongodb_default')
    mongo_client = mongo_hook.get_conn()
    db = mongo_client[Variable.get("mongodb_database", "supply_chain_analytics")]
    collection = db[Variable.get("mongodb_collection", "scheduler_table")]

    from bson.objectid import ObjectId

    mongo_ids = []
    for _id_str in loaded_ids:
        try:
            mongo_ids.append(ObjectId(_id_str))
        except Exception:
            mongo_ids.append(_id_str)

    result = collection.update_many(
        {'_id': {'$in': mongo_ids}, 'processing_status': 'loaded_to_bq'},
        {'$set': {'processing_status': 'transformed', 'transformed_at': datetime.utcnow().isoformat()}}
    )

    logging.info(f"✓ Successfully transformed {result.modified_count} records in MongoDB")

    return result.modified_count


def load_to_bigquery(**context):
    """Load MongoDB data to BigQuery (shipments_raw) for THIS RUN and push IDs via XCom"""
    logging.info("Starting MongoDB to BigQuery load (per-run)...")

    import re

    mongo_hook = MongoHook(conn_id='mongodb_default')
    mongo_client = mongo_hook.get_conn()

    db_name = Variable.get("mongodb_database", "supply_chain_analytics")
    collection_name = Variable.get("mongodb_collection", "scheduler_table")

    db = mongo_client[db_name]
    collection = db[collection_name]

    # 🔍 Debug: log distinct statuses
    try:
        statuses = collection.distinct("processing_status")
        total = collection.count_documents({})
        logging.info(f"Mongo debug → total docs: {total}, distinct processing_status: {statuses}")
    except Exception as e:
        logging.warning(f"Could not inspect Mongo statuses: {e}")

    # Now filter
    records = list(collection.find({'processing_status': 'success'}))

    if not records:
        logging.info("No processed records found in MongoDB with status 'success'")
        # Also push empty list so downstream step doesn't crash
        context['task_instance'].xcom_push(key='loaded_ids', value=[])
        return 0

    logging.info(f"Found {len(records)} records to load to BigQuery")

    # Flatten nested JSON using json_normalize
    df = pd.json_normalize(records)

    # Store IDs for this run (stringify for BQ & XCom)
    inserted_ids = []
    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)
        inserted_ids = df['_id'].tolist()
    else:
        logging.warning("No _id column in DataFrame after normalization")

    # Sanitize column names for BigQuery
    def sanitize_bq_column(col: str) -> str:
        col = col.replace(".", "_")
        col = re.sub(r"[^A-Za-z0-9_]", "_", col)
        if not re.match(r"[A-Za-z_]", col):
            col = f"_{col}"
        col = re.sub(r"__+", "_", col)
        return col[:300]

    df.columns = [sanitize_bq_column(c) for c in df.columns]

    logging.info(f"DataFrame shape: {df.shape}")
    logging.info(f"Columns: {df.columns.tolist()}")

    # 🔧 Drop airflow_run_id because it's not in the shipments_raw schema
    if 'airflow_run_id' in df.columns:
        logging.info("Dropping column 'airflow_run_id' to match BigQuery schema")
        df = df.drop(columns=['airflow_run_id'])

    # Get BigQuery connection
    bq_hook = BigQueryHook(gcp_conn_id='bigquery_default', use_legacy_sql=False)
    client = bq_hook.get_client()

    project_id = Variable.get("bigquery_project")
    dataset_id = Variable.get("bigquery_dataset")
    table_id = Variable.get("bigquery_table", "shipments_raw")

    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
    )

    logging.info(f"Loading {len(df)} records to {full_table_id}")

    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()



    logging.info(f"✓ Successfully loaded {len(df)} records to BigQuery")

    # Update MongoDB status for these records
    from bson.objectid import ObjectId

    mongo_ids = []
    for rec in records:
        _id = rec.get('_id')
        mongo_ids.append(_id)

    collection.update_many(
        {'_id': {'$in': mongo_ids}, 'processing_status': 'success'},
        {'$set': {'processing_status': 'loaded_to_bq', 'bq_loaded_at': datetime.utcnow().isoformat()}}
    )

    # Push IDs to XCom for transform step
    ti = context['task_instance']
    ti.xcom_push(key='loaded_ids', value=inserted_ids)

    logging.info(f"✓ Pushed {len(inserted_ids)} loaded_ids to XCom")

    return len(records)



def run_ml_predictions(**context):
    """Run ML predictions on new data"""
    logging.info("Starting ML predictions...")

    # Get BigQuery connection - use the same pattern as transform task
    try:
        bq_hook = BigQueryHook(gcp_conn_id='bigquery_default', use_legacy_sql=False)
        client = bq_hook.get_client()
    except Exception as e:
        logging.warning(f"Could not get BigQuery client via hook: {e}")
        # Fallback to direct client creation
        client = bigquery.Client()

    project_id = Variable.get("bigquery_project")
    dataset_id = Variable.get("bigquery_dataset")
    model_name = Variable.get("ml_model_name", "delay_regressor_v6")

    # First, get the actual column names from test_table_airflow
    schema_query = f"""
    SELECT column_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'test_table_airflow'
    """
    
    columns_df = client.query(schema_query).to_dataframe()
    all_columns = columns_df['column_name'].tolist()
    
    logging.info(f"Columns in test_table_airflow: {all_columns}")
    
    # Identify columns to exclude (label columns and identifiers)
    exclude_patterns = ['label_', 'is_delayed', '_id', 'event_id', 'timestamp']
    columns_to_exclude = [col for col in all_columns if any(pattern in col for pattern in exclude_patterns)]
    
    logging.info(f"Excluding columns: {columns_to_exclude}")
    
    # Build the EXCEPT clause
    except_clause = ", ".join(columns_to_exclude) if columns_to_exclude else "_id"

    # Model was trained with: * EXCEPT(timestamp, _id, event_id, label_delay_hours_raw)
    # So we need to exclude exactly those columns for prediction
    # The model expects label_delay_hours_capped and is_delayed as features (even though they're derived from labels)
    
    columns_to_exclude_for_prediction = ['timestamp', '_id', 'event_id', 'label_delay_hours_raw', 'label_delay_hours_capped']
    
    # Get features that match the training data
    feature_columns = [col for col in all_columns if col not in columns_to_exclude_for_prediction]
    feature_list = ", ".join(feature_columns)
    
    logging.info(f"Using {len(feature_columns)} features for prediction (matching training): {feature_columns}")

    # We need to include a unique identifier in the prediction input so we can join back
    # Use ROW_NUMBER as a unique key since _id is excluded
    query = f"""
    WITH predictions AS (
      SELECT
        row_num,
        predicted_label_delay_hours_capped AS predicted_delay_hours,
        CASE
          WHEN predicted_label_delay_hours_capped > 0.5 THEN 'DELAYED'
          WHEN predicted_label_delay_hours_capped < -0.5 THEN 'EARLY'
          ELSE 'ON_TIME'
        END AS prediction_status
      FROM ML.PREDICT(
        MODEL `{project_id}.{dataset_id}.{model_name}`,
        (
          SELECT ROW_NUMBER() OVER() as row_num, {feature_list}
          FROM `{project_id}.{dataset_id}.test_table_airflow`
        )
      )
    ),
    original_data AS (
      SELECT 
        ROW_NUMBER() OVER() as row_num,
        _id,
        event_id,
        timestamp
      FROM `{project_id}.{dataset_id}.test_table_airflow`
    )
    SELECT
      o._id,
      o.event_id,
      o.timestamp,
      p.predicted_delay_hours,
      p.prediction_status
    FROM predictions p
    JOIN original_data o ON p.row_num = o.row_num
    """
    
    logging.info(f"Prediction query: {query}")

    result = client.query(query).to_dataframe()

    logging.info(f"Generated {len(result)} predictions")

    summary = {
        'total_predictions': len(result),
        'delayed': len(result[result['prediction_status'] == 'DELAYED']),
        'on_time': len(result[result['prediction_status'] == 'ON_TIME']),
        'early': len(result[result['prediction_status'] == 'EARLY'])
    }

    context['task_instance'].xcom_push(key='prediction_summary', value=summary)
    return summary


def append_to_test_table(**context):
    """Append the new row from test_table_airflow to test_table"""
    logging.info("Appending new data to test_table...")
    
    # Get BigQuery connection
    bq_hook = BigQueryHook(gcp_conn_id='bigquery_default', use_legacy_sql=False)
    client = bq_hook.get_client()
    
    project_id = Variable.get("bigquery_project")
    dataset_id = Variable.get("bigquery_dataset")
    
    # Get the IDs that were just processed
    ti = context['task_instance']
    loaded_ids = ti.xcom_pull(task_ids='load_to_bigquery', key='loaded_ids')
    
    if not loaded_ids:
        logging.warning("No loaded_ids found. Nothing to append.")
        return 0
    
    # Build IN list for BigQuery
    id_list_sql = ", ".join([f"'{i}'" for i in loaded_ids])
    
    # Insert the new rows from test_table_airflow to test_table
    append_query = f"""
    INSERT INTO `{project_id}.{dataset_id}.test_table`
    SELECT * FROM `{project_id}.{dataset_id}.test_table_airflow`
    WHERE _id IN ({id_list_sql})
    """
    
    try:
        query_job = client.query(append_query)
        query_job.result()  # Wait for completion
        
        logging.info(f"✓ Successfully appended {len(loaded_ids)} rows to test_table")
        return len(loaded_ids)
        
    except Exception as e:
        logging.error(f"Error appending to test_table: {str(e)}")
        # Don't fail the pipeline if this fails
        return 0


def notify_completion(**context):
    """Log pipeline completion"""
    ti = context['task_instance']
    summary = ti.xcom_pull(task_ids='run_ml_predictions', key='prediction_summary')
    append_count = ti.xcom_pull(task_ids='append_to_test_table')
    
    logging.info("=" * 50)
    logging.info("Pipeline completed successfully!")
    logging.info(f"Prediction Summary: {summary}")
    logging.info(f"Appended {append_count} rows to test_table")
    logging.info("=" * 50)
    
    return "Pipeline completed"

# Define tasks
extract_task = PythonOperator(
    task_id='extract_from_google_sheets',
    python_callable=extract_from_google_sheets,
    dag=dag,
)

load_mongo_task = PythonOperator(
    task_id='load_to_mongodb_via_kafka',
    python_callable=load_to_mongodb_via_kafka,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_and_load_to_bigquery',
    python_callable=transform_and_load_to_bigquery,
    dag=dag,
)

load_bq_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag,
)

predict_task = PythonOperator(
    task_id='run_ml_predictions',
    python_callable=run_ml_predictions,
    dag=dag,
)

append_task = PythonOperator(
    task_id='append_to_test_table',
    python_callable=append_to_test_table,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='notify_completion',
    python_callable=notify_completion,
    dag=dag,
)

# Define task dependencies
# Flow: Extract → MongoDB → Load Raw to BQ → Transform & Load to test_table_airflow → ML Predict → Append to test_table → Notify
extract_task >> load_mongo_task >> load_bq_task >> transform_task >> predict_task >> append_task >> notify_task