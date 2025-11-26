# 🏗️ Architecture Documentation

> **Deep dive into the Supply Chain Analytics Platform architecture**

This document explains the technical architecture, design decisions, and data flow of the platform.

---

## 📐 System Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│  ┌──────────────┐                                               │
│  │ Google Sheets│ ← Manual data entry by operations team        │
│  └──────┬───────┘                                               │
└─────────┼─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION LAYER                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Apache Airflow (Docker Compose)                         │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐        │  │
│  │  │ Scheduler  │  │  Webserver │  │   Worker   │        │  │
│  │  └────────────┘  └────────────┘  └────────────┘        │  │
│  │  ┌────────────┐  ┌────────────┐                         │  │
│  │  │ PostgreSQL │  │   Redis    │                         │  │
│  │  └────────────┘  └────────────┘                         │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────┬───────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STREAMING LAYER                             │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Confluent Kafka (Cloud)                                 │  │
│  │  ┌────────────────────────────────────────────────────┐ │  │
│  │  │  Topic: supply_chain                               │ │  │
│  │  │  - Partitions: 1                                   │ │  │
│  │  │  - Replication: 3                                  │ │  │
│  │  │  - Retention: 7 days                               │ │  │
│  │  └────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────┬───────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                               │
│  ┌──────────────────┐         ┌──────────────────┐             │
│  │  MongoDB Atlas   │         │  Google BigQuery │             │
│  │  ┌────────────┐  │         │  ┌────────────┐  │             │
│  │  │ Raw Events │  │         │  │shipments   │  │             │
│  │  │ (Document) │  │         │  │_raw        │  │             │
│  │  └────────────┘  │         │  └────────────┘  │             │
│  │  ┌────────────┐  │         │  ┌────────────┐  │             │
│  │  │  Status    │  │         │  │test_table  │  │             │
│  │  │  Tracking  │  │         │  │_airflow    │  │             │
│  │  └────────────┘  │         │  └────────────┘  │             │
│  └──────────────────┘         │  ┌────────────┐  │             │
│                                │  │test_table  │  │             │
│                                │  │(accumulated)│  │             │
│                                │  └────────────┘  │             │
│                                └──────────────────┘             │
└─────────┬───────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   TRANSFORMATION LAYER                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  dbt (Data Build Tool)                                   │  │
│  │  ┌────────────────────────────────────────────────────┐ │  │
│  │  │  Models:                                           │ │  │
│  │  │  1. stg_shipments (staging)                        │ │  │
│  │  │  2. shipment_metrics (feature engineering)         │ │  │
│  │  │     - Temporal features                            │ │  │
│  │  │     - Interaction features                         │ │  │
│  │  │     - Categorical buckets                          │ │  │
│  │  │     - Aggregations                                 │ │  │
│  │  └────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────┬───────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ML/AI LAYER                                 │
│  ┌──────────────────┐         ┌──────────────────┐             │
│  │  BigQuery ML     │         │    Euri AI       │             │
│  │  ┌────────────┐  │         │  ┌────────────┐  │             │
│  │  │  Model:    │  │         │  │  GPT-4.1   │  │             │
│  │  │  delay_    │  │         │  │   Nano     │  │             │
│  │  │  regressor │  │         │  └────────────┘  │             │
│  │  │  _v6       │  │         │  Natural Language│             │
│  │  └────────────┘  │         │  Responses       │             │
│  │  Linear Regression│        └──────────────────┘             │
│  │  40+ Features    │                                           │
│  └──────────────────┘                                           │
└─────────┬───────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PRESENTATION LAYER                             │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Streamlit Web Application                               │  │
│  │  ┌────────────────┐         ┌────────────────┐          │  │
│  │  │  Admin Portal  │         │  Client Portal │          │  │
│  │  │  - Analytics   │         │  - Order Track │          │  │
│  │  │  - Monitoring  │         │  - AI Chat     │          │  │
│  │  │  - Scheduling  │         │  - Predictions │          │  │
│  │  └────────────────┘         └────────────────┘          │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow

### End-to-End Pipeline

#### Phase 1: Data Ingestion

```
Google Sheets
    │
    │ (1) Airflow reads one row
    │     - Tracks last_processed_row_index
    │     - Incremental processing
    │
    ▼
Airflow Task: extract_from_google_sheets
    │
    │ Output: Single record (dict)
    │ XCom: extracted_data, row_index
    │
    ▼
```

#### Phase 2: Streaming & Storage

```
Airflow Task: load_to_mongodb_via_kafka
    │
    ├─► (2a) Transform to nested JSON
    │        - vehicle_data
    │        - operational_metrics
    │        - external_factors
    │        - performance_indicators
    │        - temporal_features
    │
    ├─► (2b) Send to Kafka (optional)
    │        - Topic: supply_chain
    │        - Key: event_id
    │        - Value: JSON message
    │        - Timeout: 5 seconds
    │
    └─► (2c) Insert to MongoDB
             - Collection: scheduler_table
             - Status: 'success'
             - Timestamp: ingestion_timestamp
```

#### Phase 3: Data Warehouse Loading

```
Airflow Task: load_to_bigquery
    │
    ├─► (3a) Query MongoDB
    │        - Filter: processing_status = 'success'
    │        - Flatten nested JSON
    │        - Sanitize column names
    │
    ├─► (3b) Load to BigQuery
    │        - Table: shipments_raw
    │        - Mode: APPEND
    │        - Auto-detect schema
    │
    └─► (3c) Update MongoDB status
             - Status: 'loaded_to_bq'
             - Timestamp: bq_loaded_at
             - Push loaded_ids to XCom
```

#### Phase 4: Feature Engineering

```
Airflow Task: transform_and_load_to_bigquery
    │
    ├─► (4a) Create stg_shipments view
    │        - Flatten shipments_raw
    │        - Filter by loaded_ids (this run only)
    │
    ├─► (4b) Run dbt transformations
    │        - Apply shipment_metrics model
    │        - Generate 40+ features:
    │          * Temporal: hour_of_day, day_of_week, etc.
    │          * Cyclical: sin_hour, cos_hour, etc.
    │          * Interaction: traffic_x_weather, etc.
    │          * Categorical: traffic_bucket, etc.
    │          * Aggregations: avg_delay_region4_hour, etc.
    │
    ├─► (4c) Load to test_table_airflow
    │        - Temporary table for this run
    │        - Same schema as shipment_metrics
    │
    └─► (4d) Update MongoDB status
             - Status: 'transformed'
             - Timestamp: transformed_at
```

#### Phase 5: ML Prediction

```
Airflow Task: run_ml_predictions
    │
    ├─► (5a) Get feature columns
    │        - Query INFORMATION_SCHEMA
    │        - Exclude: _id, event_id, timestamp, label_*
    │
    ├─► (5b) Run ML.PREDICT
    │        - Model: delay_regressor_v6
    │        - Input: test_table_airflow
    │        - Output: predicted_delay_hours, prediction_status
    │
    └─► (5c) Return predictions
             - DELAYED: > 0.5 hours
             - ON_TIME: -0.5 to 0.5 hours
             - EARLY: < -0.5 hours
```

#### Phase 6: Data Accumulation

```
Airflow Task: append_to_test_table
    │
    └─► (6) Append to test_table
         - Insert from test_table_airflow
         - Filter by loaded_ids
         - Accumulate historical data
         - Used for dashboard analytics
```

---

## 🗄️ Data Models

### MongoDB Schema

**Collection: scheduler_table**

```javascript
{
  _id: ObjectId("673e6b6e3c245..."),
  event_id: "evt_row_123_1234567890",
  timestamp: "2024-01-01T00:00:00",
  
  vehicle_data: {
    gps_latitude: 0.34,
    gps_longitude: 0.75,
    eta_variation_hours: 0.93
  },
  
  operational_metrics: {
    traffic_congestion_level: 0.27,
    loading_unloading_time: 1.71,
    handling_equipment_availability: 0.55,
    order_fulfillment_status: 0.46
  },
  
  external_factors: {
    weather_condition_severity: -0.39,
    port_congestion_level: -0.83,
    shipping_costs: -0.01
  },
  
  performance_indicators: {
    lead_time_days: -0.69,
    disruption_likelihood_score: -1.07,
    delay_probability: 0.57,
    risk_classification: 2.14,
    delivery_time_deviation: 0.95
  },
  
  temporal_features: {
    hour: 12,
    day: 15,
    month: 6,
    weekday: 2
  },
  
  // Pipeline metadata
  ingestion_timestamp: "2024-01-01T12:00:00Z",
  processing_status: "success",  // success → loaded_to_bq → transformed
  processed_timestamp: "2024-01-01T12:00:01Z",
  bq_loaded_at: "2024-01-01T12:00:05Z",
  transformed_at: "2024-01-01T12:00:10Z"
}
```

**Indexes:**
```javascript
db.scheduler_table.createIndex({ "processing_status": 1 })
db.scheduler_table.createIndex({ "event_id": 1 }, { unique: true })
db.scheduler_table.createIndex({ "timestamp": -1 })
```

### BigQuery Schema

**Table: shipments_raw**

```sql
CREATE TABLE `project.supply_chain.shipments_raw` (
  _id STRING,
  event_id STRING,
  timestamp TIMESTAMP,
  ingestion_timestamp TIMESTAMP,
  processed_timestamp TIMESTAMP,
  processing_status STRING,
  
  -- Flattened nested fields
  vehicle_data_gps_latitude FLOAT64,
  vehicle_data_gps_longitude FLOAT64,
  vehicle_data_eta_variation_hours FLOAT64,
  
  operational_metrics_traffic_congestion_level FLOAT64,
  operational_metrics_loading_unloading_time FLOAT64,
  operational_metrics_handling_equipment_availability FLOAT64,
  operational_metrics_order_fulfillment_status FLOAT64,
  
  external_factors_weather_condition_severity FLOAT64,
  external_factors_port_congestion_level FLOAT64,
  external_factors_shipping_costs FLOAT64,
  
  performance_indicators_lead_time_days FLOAT64,
  performance_indicators_disruption_likelihood_score FLOAT64,
  performance_indicators_delay_probability FLOAT64,
  performance_indicators_risk_classification FLOAT64,
  performance_indicators_delivery_time_deviation FLOAT64,
  
  temporal_features_hour INT64,
  temporal_features_day INT64,
  temporal_features_month INT64,
  temporal_features_weekday INT64
);
```

**View: stg_shipments**

```sql
CREATE OR REPLACE VIEW `project.supply_chain.stg_shipments` AS
SELECT
  _id,
  event_id,
  timestamp,
  
  -- Rename flattened columns to simple names
  vehicle_data_gps_latitude AS gps_latitude,
  vehicle_data_gps_longitude AS gps_longitude,
  vehicle_data_eta_variation_hours AS eta_variation_hours,
  
  operational_metrics_traffic_congestion_level AS traffic_congestion_level,
  -- ... other columns
  
FROM `project.supply_chain.shipments_raw`;
```

**View: shipment_metrics (dbt model)**

```sql
-- 40+ engineered features
SELECT
  _id,
  event_id,
  timestamp,
  
  -- Base features
  gps_latitude,
  gps_longitude,
  traffic_congestion_level,
  -- ... other base features
  
  -- Temporal features
  EXTRACT(HOUR FROM timestamp) AS hour_of_day,
  EXTRACT(DAYOFWEEK FROM timestamp) AS day_of_week,
  EXTRACT(MONTH FROM timestamp) AS month_of_year,
  EXTRACT(ISOWEEK FROM timestamp) AS iso_week,
  CASE WHEN EXTRACT(DAYOFWEEK FROM timestamp) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
  CASE WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 7 AND 9 
       OR EXTRACT(HOUR FROM timestamp) BETWEEN 17 AND 19 THEN 1 ELSE 0 END AS is_rush_hour,
  
  -- Cyclical encoding
  SIN(2 * 3.14159 * EXTRACT(HOUR FROM timestamp) / 24) AS sin_hour,
  COS(2 * 3.14159 * EXTRACT(HOUR FROM timestamp) / 24) AS cos_hour,
  SIN(2 * 3.14159 * EXTRACT(MONTH FROM timestamp) / 12) AS sin_month,
  COS(2 * 3.14159 * EXTRACT(MONTH FROM timestamp) / 12) AS cos_month,
  
  -- Geographic features
  CONCAT(
    CAST(FLOOR(gps_latitude * 10) AS STRING), '_',
    CAST(FLOOR(gps_longitude * 10) AS STRING)
  ) AS region4,
  
  -- Interaction features
  traffic_congestion_level * loading_unloading_time AS cong_x_loading,
  traffic_congestion_level * weather_condition_severity AS traffic_x_weather,
  loading_unloading_time * handling_equipment_availability AS load_x_equipment,
  port_congestion_level * traffic_congestion_level AS port_x_traffic,
  traffic_congestion_level * disruption_likelihood_score AS traffic_x_disruption,
  lead_time_days * port_congestion_level AS leadtime_x_port,
  weather_condition_severity * lead_time_days AS weather_x_leadtime,
  
  -- Categorical buckets
  CASE
    WHEN traffic_congestion_level < -0.5 THEN 'low'
    WHEN traffic_congestion_level < 0.5 THEN 'medium'
    ELSE 'high'
  END AS traffic_bucket,
  
  CASE
    WHEN loading_unloading_time < 0 THEN 'fast'
    WHEN loading_unloading_time < 1 THEN 'normal'
    ELSE 'slow'
  END AS loading_time_bucket,
  
  -- ... other categorical buckets
  
  -- Aggregations (window functions)
  AVG(delay_probability) OVER (
    PARTITION BY region4, EXTRACT(HOUR FROM timestamp)
  ) AS avg_delay_region4_hour,
  
  AVG(delay_probability) OVER (
    PARTITION BY region4, EXTRACT(DAYOFWEEK FROM timestamp)
  ) AS avg_delay_region4_day,
  
  -- Label (target variable)
  delivery_time_deviation AS label_delay_hours_raw,
  CASE
    WHEN delivery_time_deviation > 10 THEN 10
    WHEN delivery_time_deviation < -10 THEN -10
    ELSE delivery_time_deviation
  END AS label_delay_hours_capped,
  
  CASE
    WHEN delivery_time_deviation > 0.5 THEN 1
    ELSE 0
  END AS is_delayed,
  
  CASE
    WHEN delivery_time_deviation > 5 THEN 1
    ELSE 0
  END AS is_severe_delay

FROM `project.supply_chain.stg_shipments`;
```

**Table: test_table**

```sql
-- Accumulated predictions over time
-- Same schema as shipment_metrics
-- Used for dashboard analytics
```

---

## 🤖 ML Model Architecture

### Model: delay_regressor_v6

**Type:** Linear Regression (BigQuery ML)

**Training Query:**
```sql
CREATE OR REPLACE MODEL `project.supply_chain.delay_regressor_v6`
OPTIONS(
  model_type='LINEAR_REG',
  input_label_cols=['label_delay_hours_capped'],
  data_split_method='AUTO_SPLIT',
  data_split_eval_fraction=0.2,
  l2_reg=0.1,
  max_iterations=50,
  learn_rate_strategy='LINE_SEARCH',
  early_stop=TRUE,
  min_rel_progress=0.01
) AS
SELECT * EXCEPT(timestamp, _id, event_id, label_delay_hours_raw)
FROM `project.supply_chain.shipment_metrics`
WHERE label_delay_hours_capped IS NOT NULL;
```

**Features (40+):**

1. **Base Features (14):**
   - gps_latitude, gps_longitude
   - traffic_congestion_level
   - loading_unloading_time
   - handling_equipment_availability
   - order_fulfillment_status
   - weather_condition_severity
   - port_congestion_level
   - shipping_costs
   - lead_time_days
   - disruption_likelihood_score
   - delay_probability
   - risk_classification
   - delivery_time_deviation

2. **Temporal Features (10):**
   - hour_of_day, day_of_week, month_of_year, iso_week
   - is_weekend, is_rush_hour
   - sin_hour, cos_hour, sin_month, cos_month

3. **Geographic Features (2):**
   - region4, region5

4. **Interaction Features (7):**
   - cong_x_loading
   - traffic_x_weather
   - load_x_equipment
   - port_x_traffic
   - traffic_x_disruption
   - leadtime_x_port
   - weather_x_leadtime

5. **Categorical Buckets (6):**
   - traffic_bucket
   - loading_time_bucket
   - handling_availability_bucket
   - weather_bucket
   - port_congestion_bucket
   - lead_time_bucket

6. **Aggregations (3):**
   - avg_delay_region4_hour
   - avg_delay_region4_day
   - avg_delay_region4_week

7. **Derived Labels (2):**
   - is_delayed
   - is_severe_delay

**Prediction Query:**
```sql
SELECT
  _id,
  event_id,
  predicted_label_delay_hours_capped AS predicted_delay_hours,
  CASE
    WHEN predicted_label_delay_hours_capped > 0.5 THEN 'DELAYED'
    WHEN predicted_label_delay_hours_capped < -0.5 THEN 'EARLY'
    ELSE 'ON_TIME'
  END AS prediction_status
FROM ML.PREDICT(
  MODEL `project.supply_chain.delay_regressor_v6`,
  (SELECT * EXCEPT(_id, event_id, timestamp, label_delay_hours_raw)
   FROM `project.supply_chain.test_table_airflow`)
);
```

**Model Evaluation:**
```sql
SELECT
  mean_absolute_error,
  mean_squared_error,
  mean_squared_log_error,
  median_absolute_error,
  r2_score,
  explained_variance
FROM ML.EVALUATE(
  MODEL `project.supply_chain.delay_regressor_v6`,
  (SELECT * EXCEPT(timestamp, _id, event_id, label_delay_hours_raw)
   FROM `project.supply_chain.shipment_metrics`
   WHERE label_delay_hours_capped IS NOT NULL)
);
```

---

## 🔐 Security Architecture

### Authentication & Authorization

**User Management:**
- Password hashing: SHA-256
- Storage: JSON file (`data/users.json`)
- Roles: `admin`, `client`

**Session Management:**
- Streamlit session state
- No persistent sessions (resets on page refresh)

**API Authentication:**
- Euri AI: Bearer token
- Confluent Kafka: SASL_SSL with API key/secret
- MongoDB: Username/password with TLS
- BigQuery: Service account JSON key

### Network Security

**Airflow:**
- Runs in Docker network
- Exposed ports: 8080 (webserver)
- Internal communication via Docker DNS

**Kafka:**
- SASL_SSL protocol
- TLS encryption in transit
- API key authentication

**MongoDB:**
- TLS/SSL encryption
- IP whitelist (configurable)
- Database authentication

**BigQuery:**
- Service account with IAM roles
- Encrypted at rest
- Audit logging enabled

### Data Security

**Sensitive Data:**
- Credentials stored in `.env` files (gitignored)
- Service account keys in `configs/` (gitignored)
- Airflow connections encrypted in PostgreSQL

**Data in Transit:**
- Kafka: TLS encryption
- MongoDB: TLS encryption
- BigQuery: HTTPS

**Data at Rest:**
- MongoDB: Encrypted by default (Atlas)
- BigQuery: Encrypted by default
- Airflow metadata: PostgreSQL (can enable encryption)

---

## 📊 Performance Considerations

### Scalability

**Current Capacity:**
- **Throughput:** 120 rows/hour (30s per row)
- **Latency:** ~30 seconds end-to-end
- **Storage:** Unlimited (cloud-based)

**Bottlenecks:**
1. **Google Sheets API:** Rate limits (100 requests/100 seconds)
2. **Kafka Timeout:** 5 seconds per message
3. **BigQuery:** Query execution time
4. **Airflow:** Single worker (can scale horizontally)

**Optimization Strategies:**

1. **Batch Processing:**
   ```python
   # Instead of 1 row per run
   records = sheet.get_all_records()[last_index:last_index+10]
   # Process 10 rows per run
   ```

2. **Parallel Tasks:**
   ```python
   # Use Airflow task groups
   with TaskGroup("parallel_processing") as group:
       task1 = PythonOperator(...)
       task2 = PythonOperator(...)
   ```

3. **Caching:**
   ```python
   # Add Redis for frequently accessed data
   @st.cache_data(ttl=300)
   def get_all_shipments():
       # Cache for 5 minutes
   ```

4. **Partitioning:**
   ```sql
   -- Partition BigQuery tables by date
   CREATE TABLE `project.supply_chain.shipments_raw`
   PARTITION BY DATE(timestamp)
   CLUSTER BY event_id;
   ```

5. **Indexing:**
   ```javascript
   // MongoDB compound indexes
   db.scheduler_table.createIndex({
     "processing_status": 1,
     "timestamp": -1
   })
   ```

### Monitoring

**Airflow Metrics:**
- Task success/failure rates
- Task duration
- DAG run duration
- Queue size

**Application Metrics:**
- Dashboard load time
- Query execution time
- API response time
- Error rates

**Infrastructure Metrics:**
- Docker container CPU/memory
- PostgreSQL connections
- Redis memory usage
- Network throughput

---

## 🔄 Disaster Recovery

### Backup Strategy

**MongoDB:**
- Automated daily backups (Atlas)
- Point-in-time recovery
- Retention: 7 days

**BigQuery:**
- Table snapshots
- Time travel (7 days)
- Export to Cloud Storage

**Airflow:**
- PostgreSQL backups
- DAG version control (Git)
- Configuration backups

### Recovery Procedures

**Data Loss:**
1. Restore MongoDB from backup
2. Restore BigQuery tables from snapshots
3. Re-run Airflow DAG for missing data

**Service Outage:**
1. Check Docker containers: `docker-compose ps`
2. Restart services: `docker-compose restart`
3. Check logs: `docker-compose logs -f`

**Corruption:**
1. Identify corrupted records
2. Delete from MongoDB and BigQuery
3. Re-process from Google Sheets

---

## 🚀 Future Enhancements

### Phase 2 (Next 3 months)

1. **Real-time Streaming:**
   - Kafka consumer running 24/7
   - WebSocket updates to dashboard
   - Real-time alerts

2. **Advanced Analytics:**
   - Time series forecasting
   - Anomaly detection
   - Root cause analysis

3. **Notifications:**
   - Email alerts for delays
   - SMS notifications
   - Slack/Teams integration

### Phase 3 (6-12 months)

1. **Multi-Model Ensemble:**
   - XGBoost for non-linear patterns
   - LSTM for time series
   - Model stacking

2. **Automated Retraining:**
   - Scheduled model updates
   - A/B testing framework
   - Model versioning

3. **Mobile App:**
   - React Native app
   - Push notifications
   - Offline mode

### Phase 4 (12+ months)

1. **AI-Powered Optimization:**
   - Route optimization
   - Resource allocation
   - Demand forecasting

2. **Blockchain Integration:**
   - Immutable audit trail
   - Smart contracts
   - Supply chain transparency

3. **IoT Integration:**
   - Real-time GPS tracking
   - Temperature sensors
   - RFID tags

---

## 📚 References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Confluent Kafka Documentation](https://docs.confluent.io/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [BigQuery ML Documentation](https://cloud.google.com/bigquery-ml/docs)
- [dbt Documentation](https://docs.getdbt.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)

---

**Version:** 1.0.0
