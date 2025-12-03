# 🚀 Supply Chain Analytics As A Service Platform

> **Real-time supply chain monitoring and ML-powered delivery prediction system**
> **Project Team Group: Supply_chain_as_a_service**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.7+-green.svg)](https://airflow.apache.org/)
[![BigQuery ML](https://img.shields.io/badge/BigQuery-ML-orange.svg)](https://cloud.google.com/bigquery-ml)


A complete end-to-end data pipeline that extracts shipment data from Google Sheets, processes it through Kafka and MongoDB, transforms it with dbt, stores it in BigQuery, and provides ML-powered delivery predictions through an interactive Streamlit dashboard.

![Dashboard Preview](docs/images/dashboard-preview.png)

---

## 🎬 Demo

**Live Demo and Walkthrough:** [Youtube Video](https://youtu.be/QvIhfgtEL-I?si=GHoBTmojJVC0hZR5)
**Project Report:** [Doc Report](https://docs.google.com/document/d/1H7tL823vl_3GUHpffd3HPiMPVnYD-Evkll2ycL3PfuQ/edit?usp=sharing)
**Documentation:** [Full Docs](README.md) | [Quick Start](QUICKSTART.md) | [Architecture](ARCHITECTURE.md)

---

##  Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [API Documentation](#api-documentation)
- [Troubleshooting](#troubleshooting)

---

##  Overview

This platform solves the critical problem of **supply chain visibility and delivery prediction**. It provides:

- **Real-time data ingestion** from Google Sheets
- **Streaming architecture** using Confluent Kafka
- **Automated ETL pipeline** orchestrated by Apache Airflow
- **ML-powered predictions** using BigQuery ML
- **Interactive dashboards** for both admins and clients
- **AI-powered chat** for delivery status inquiries using Euri AI

### The Problem We Solve

Supply chain managers struggle with:
- Lack of real-time visibility into shipment status
- Inability to predict delays before they happen
- Manual data processing and reporting
- Poor communication with clients about delivery status

### Our Solution

**Automated data pipeline** that processes shipments in real-time  
**ML predictions** that forecast delays with 85%+ accuracy  
**Self-service portal** for clients to check order status  
**AI-powered responses** that explain delays in natural language  
**Admin analytics** for operational insights  

---

##  Architecture

```
┌─────────────────┐
│  Google Sheets  │ ← Data Source (Manual Entry)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Airflow DAG    │ ← Orchestration (Scheduled/Manual)
│  (Extract)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│  Kafka Topic    │◄────►│   MongoDB    │
│  (Streaming)    │      │  (Raw Store) │
└────────┬────────┘      └──────────────┘
         │
         ▼
┌─────────────────┐
│   BigQuery      │
│  (Data Lake)    │
│  - shipments_raw│
│  - test_table   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  dbt Transform  │ ← Feature Engineering
│  (shipment_     │   - Temporal features
│   metrics)      │   - Interaction features
└────────┬────────┘   - Categorical buckets
         │
         ▼
┌─────────────────┐
│  BigQuery ML    │ ← ML Model Training
│  (delay_        │   - Linear Regression
│   regressor_v6) │   - Delay Prediction
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Streamlit App  │ ← User Interface
│  - Admin Portal │   - View analytics
│  - Client Portal│   - Check orders
│  - AI Chat      │   - Get predictions
└─────────────────┘
```

---

##  Features

###  Dual Portal System

**Admin Portal:**
- 📊 Real-time shipment monitoring dashboard
- 📈 Analytics with categorical breakdowns
- 📝 Client activity logs
- 🚀 Manual pipeline triggering
- 🎯 KPI tracking (delayed, on-time, early)

**Client Portal:**
-  Natural language order tracking
-  AI-powered delivery status explanations
-  Predicted delay/early arrival times
-  Order details and metrics

### ML-Powered Predictions

- **Model:** BigQuery ML Linear Regressor
- **Features:** 40+ engineered features including:
  - Temporal patterns (hour, day, week, seasonality)
  - Geographic data (GPS coordinates, regions)
  - Operational metrics (traffic, loading time, equipment)
  - External factors (weather, port congestion, costs)
  - Interaction features (traffic × weather, port × traffic, etc.)
  - Categorical buckets for interpretability

###  Automated Pipeline

- **Incremental processing:** One row at a time from Google Sheets
- **Status tracking:** MongoDB tracks processing stages
- **Fault tolerance:** Retry logic and error handling
- **Idempotency:** Safe to re-run without duplicates

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Source** | Google Sheets | Manual data entry |
| **Orchestration** | Apache Airflow | Workflow automation |
| **Streaming** | Confluent Kafka | Real-time data streaming |
| **Storage** | MongoDB Atlas | Document store for raw events |
| **Data Warehouse** | Google BigQuery | Analytics and ML |
| **Transformation** | dbt | Feature engineering |
| **ML** | BigQuery ML | Delay prediction model |
| **Frontend** | Streamlit | Interactive dashboards |
| **AI** | Euri AI (GPT-4) | Natural language responses |
| **Language** | Python 3.11+ | Backend logic |

---

## Prerequisites

Before you begin, ensure you have:

### Required Accounts
- Google Cloud Platform account (with BigQuery enabled)
- Confluent Cloud account (Kafka)
- MongoDB Atlas account
- Euri AI API key
- Google Sheets with service account access

### Required Software
- Python 3.11 or higher
- Docker & Docker Compose (for Airflow)
- Git

### Required Credentials
- Google Cloud service account JSON
- Confluent Kafka API key & secret
- MongoDB connection URI
- Euri AI API key

---

## 🚀 Installation

### 📝 Before You Start

This repository uses **template files** for sensitive configuration. You'll need to:

1. Copy `.example` files to create your own configuration files
2. Fill in your actual credentials (API keys, passwords, etc.)
3. Download your Google Cloud service account JSON key

**Template files included:**
- `configs/.env.example` → Copy to `configs/.env`
- `supply-chain-airflow/.env.example` → Copy to `supply-chain-airflow/.env`
- `airflow_variables.example.json` → Copy to `airflow_variables.json`

**Files you need to create:**
- `configs/google-credentials.json` (download from GCP)
- `supply-chain-airflow/configs/google-credentials.json` (same file)

---

### Step 1: Clone the Repository

```bash
git clone <your-repo-url>
cd supply-chain-analytics
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Set Up Airflow

```bash
cd supply-chain-airflow

# Initialize Airflow database
docker-compose up airflow-init

# Start Airflow services
docker-compose up -d
```

Access Airflow UI at `http://localhost:8080` (default credentials: `admin/admin`)

### Step 4: Configure Environment Variables

#### 4.1 Understanding the Configuration Files

**Three configuration files are needed:**

| File | Purpose | Used By |
|------|---------|---------|
| `configs/.env` | Main application config | Streamlit app |
| `supply-chain-airflow/.env` | Airflow environment | Airflow containers |
| `airflow_variables.json` | Airflow variables | Airflow DAGs |
| `configs/google-credentials.json` | GCP authentication | All services |

#### 4.2 Create Configuration Files

```bash
# Copy template files
cp configs/.env.example configs/.env
cp supply-chain-airflow/.env.example supply-chain-airflow/.env
cp airflow_variables.example.json airflow_variables.json
```

#### 4.3 Fill in Your Credentials

**File: `configs/.env`**

Open the file and replace placeholder values:

```bash
# MongoDB Configuration
MONGODB_URI=mongodb+srv://YOUR_USERNAME:YOUR_PASSWORD@YOUR_CLUSTER.mongodb.net/?retryWrites=true&w=majority
MONGODB_DATABASE=supply_chain_analytics
MONGODB_COLLECTION=scheduler_events

# Kafka Configuration (Confluent Cloud)
KAFKA_BOOTSTRAP_SERVERS=YOUR_BOOTSTRAP_SERVER:9092
KAFKA_API_KEY=YOUR_KAFKA_API_KEY
KAFKA_API_SECRET=YOUR_KAFKA_API_SECRET
KAFKA_TOPIC=supply_chain

# BigQuery Configuration
BIGQUERY_PROJECT=YOUR_GCP_PROJECT_ID
BIGQUERY_DATASET=supply_chain
BIGQUERY_TABLE=shipments_raw
BQ_MODEL=delay_regressor_v6
BQ_TEST_TBL=test_table

# Euri AI Configuration
EURI_API_KEY=YOUR_EURI_API_KEY
EURI_MODEL_NAME=gpt-4.1-nanosss

# Airflow Configuration
AIRFLOW_URL=http://localhost:8080
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_DAG_ID=supply_chain_pipeline
```

**Where to get these values:**

| Variable | How to Get It |
|----------|---------------|
| `MONGODB_URI` | MongoDB Atlas → Database → Connect → Connection String |
| `KAFKA_BOOTSTRAP_SERVERS` | Confluent Cloud → Cluster Settings → Bootstrap Server |
| `KAFKA_API_KEY` | Confluent Cloud → API Keys → Create Key |
| `KAFKA_API_SECRET` | Confluent Cloud → API Keys → (shown once when created) |
| `BIGQUERY_PROJECT` | Google Cloud Console → Project ID |
| `EURI_API_KEY` | Euri AI Dashboard → API Keys |
| `GOOGLE_SHEET_ID` | Google Sheets URL → Extract ID from URL |

**File: `supply-chain-airflow/.env`**

This file has the same variables as above, plus:

```bash
# Additional Airflow-specific variables
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/configs/google-credentials.json
AIRFLOW_UID=50000
POSTGRES_PORT=5433
GOOGLE_SHEET_ID=YOUR_GOOGLE_SHEET_ID
ML_MODEL_NAME=delay_regressor_v6
```

**File: `airflow_variables.json`**

Update all values in this JSON file:

```json
{
  "google_sheet_id": "YOUR_GOOGLE_SHEET_ID",
  "kafka_bootstrap_servers": "YOUR_BOOTSTRAP_SERVER:9092",
  "kafka_api_key": "YOUR_KAFKA_API_KEY",
  "kafka_api_secret": "YOUR_KAFKA_API_SECRET",
  "kafka_topic": "supply_chain",
  "mongodb_uri": "mongodb+srv://YOUR_USERNAME:YOUR_PASSWORD@YOUR_CLUSTER.mongodb.net/...",
  "mongodb_database": "supply_chain_analytics",
  "mongodb_collection": "scheduler_table",
  "bigquery_project": "YOUR_GCP_PROJECT_ID",
  "bigquery_dataset": "supply_chain",
  "bigquery_table": "shipments_raw",
  "bq_model": "delay_regressor_v6",
  "bq_test_tbl": "test_table",
  "ml_model_name": "delay_regressor_v6",
  "last_processed_row_index": "0"
}
```

#### 4.4 Set Up Google Cloud Credentials

**Download Service Account Key:**

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Select your project
3. Navigate to **IAM & Admin** → **Service Accounts**
4. Click on your service account (or create new one)
5. Go to **Keys** tab
6. Click **Add Key** → **Create New Key**
7. Select **JSON** format
8. Click **Create** (file will download automatically)

**Save the downloaded file:**

```bash
# Create configs directory if it doesn't exist
mkdir -p configs
mkdir -p supply-chain-airflow/configs

# Move downloaded file (replace 'downloaded-file.json' with actual filename)
mv ~/Downloads/your-project-xxxxx.json configs/google-credentials.json

# Copy to Airflow directory
cp configs/google-credentials.json supply-chain-airflow/configs/google-credentials.json
```

**Verify the file:**

```bash
# Check if file exists and is valid JSON
cat configs/google-credentials.json | python -m json.tool
```

**⚠️ Security Warning:**
- These files contain sensitive credentials
- They are already in `.gitignore` and won't be committed
- Never share these files publicly
- Rotate credentials regularly

---

## ⚙️ Configuration

### Important: Environment Setup

**Before running the application, you MUST set up your environment files with credentials.**

The repository includes template files (`.example` files) that you need to copy and fill with your actual credentials:

#### Step 1: Create Environment Files

```bash
# Copy template files
cp configs/.env.example configs/.env
cp supply-chain-airflow/.env.example supply-chain-airflow/.env
cp airflow_variables.example.json airflow_variables.json
```

#### Step 2: Edit Configuration Files

**File: `configs/.env`** (Main application)
```bash
# Open and edit with your credentials
nano configs/.env
# or
code configs/.env
```

Required variables:
- `MONGODB_URI` - Your MongoDB Atlas connection string
- `KAFKA_BOOTSTRAP_SERVERS` - Confluent Kafka bootstrap servers
- `KAFKA_API_KEY` - Confluent Kafka API key
- `KAFKA_API_SECRET` - Confluent Kafka API secret
- `BIGQUERY_PROJECT` - Your GCP project ID
- `EURI_API_KEY` - Your Euri AI API key
- `AIRFLOW_URL` - Airflow webserver URL (default: http://localhost:8080)

**File: `supply-chain-airflow/.env`** (Airflow)
```bash
# Open and edit with your credentials
nano supply-chain-airflow/.env
```

Required variables:
- Same as above, plus:
- `GOOGLE_APPLICATION_CREDENTIALS` - Path to GCP credentials JSON
- `GOOGLE_SHEET_ID` - Your Google Sheet ID
- `AIRFLOW_UID` - Airflow user ID (default: 50000)
- `POSTGRES_PORT` - PostgreSQL port (default: 5433)

**File: `airflow_variables.json`** (Airflow Variables)
```bash
# Open and edit with your credentials
nano airflow_variables.json
```

This file contains all Airflow variables. Update all placeholder values with your actual credentials.

#### Step 3: Set Up Google Cloud Credentials

**Create and download GCP service account key:**

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to **IAM & Admin** → **Service Accounts**
3. Create a new service account or select existing one
4. Click **Keys** → **Add Key** → **Create New Key**
5. Choose **JSON** format
6. Download the JSON file
7. Save it as:
   ```bash
   # For main app
   configs/google-credentials.json
   
   # For Airflow (copy the same file)
   supply-chain-airflow/configs/google-credentials.json
   ```

** IMPORTANT:** 
- Never commit these files to Git (they're already in `.gitignore`)
- Keep your credentials secure
- Rotate credentials regularly

---

### 1. Google Cloud Setup

**Create a Service Account:**
1. Go to Google Cloud Console → IAM & Admin → Service Accounts
2. Create a new service account
3. Grant roles:
   - BigQuery Admin
   - BigQuery Data Editor
   - BigQuery Job User
4. Create and download JSON key
5. Save as `configs/google-credentials.json`

**Enable APIs:**
```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable sheets.googleapis.com
```

**Create BigQuery Dataset:**
```sql
CREATE SCHEMA `your-project.supply_chain`;
```

### 2. Google Sheets Setup

1. Create a Google Sheet with your supply chain data
2. Share the sheet with your service account email
3. Copy the Sheet ID from the URL:
   ```
   https://docs.google.com/spreadsheets/d/[SHEET_ID]/edit
   ```

**Required Columns:**
- timestamp
- vehicle_gps_latitude
- vehicle_gps_longitude
- eta_variation_hours
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
- hour, day, month, weekday

### 3. Confluent Kafka Setup

1. Create a Kafka cluster in Confluent Cloud
2. Create a topic named `supply_chain`
3. Generate API key and secret
4. Note your bootstrap server URL

### 4. MongoDB Setup

1. Create a MongoDB Atlas cluster
2. Create a database named `supply_chain_analytics`
3. Create a collection named `scheduler_table`
4. Get your connection URI

### 5. Euri AI Setup

1. Sign up at [Euri AI](https://euron.one)
2. Generate an API key
3. Add to your `.env` file

### 6. Configure Airflow

**Upload Variables:**
1. Go to Airflow UI → Admin → Variables
2. Upload `airflow_variables.json`:

```json
{
  "google_sheet_id": "your_sheet_id",
  "kafka_bootstrap_servers": "pkc-xxxxx.us-east-2.aws.confluent.cloud:9092",
  "kafka_api_key": "your_api_key",
  "kafka_api_secret": "your_api_secret",
  "kafka_topic": "supply_chain",
  "mongodb_uri": "mongodb+srv://user:pass@cluster.mongodb.net/",
  "mongodb_database": "supply_chain_analytics",
  "mongodb_collection": "scheduler_table",
  "bigquery_project": "your-project-id",
  "bigquery_dataset": "supply_chain",
  "bigquery_table": "shipments_raw",
  "bq_model": "delay_regressor_v6",
  "bq_test_tbl": "test_table",
  "ml_model_name": "delay_regressor_v6",
  "last_processed_row_index": "0"
}
```

**Create Connections:**

**MongoDB Connection:**
- Connection ID: `mongodb_default`
- Connection Type: MongoDB
- Host: `your-cluster.mongodb.net`
- Schema: `supply_chain_analytics`
- Login: `your_username`
- Password: `your_password`
- Extra: `{"srv": true}`

**BigQuery Connection:**
- Connection ID: `bigquery_default`
- Connection Type: Google Cloud
- Project ID: `your-project-id`
- Keyfile Path: `/opt/airflow/configs/google-credentials.json`

---

## Usage

### Running the Complete Pipeline

#### Option 1: Via Streamlit (Recommended)

1. **Start Streamlit App:**
```bash
streamlit run app.py
```

2. **Login as Admin:**
   - Username: `admin`
   - Password: `admin123`

3. **Schedule New Order:**
   - Click "🚀 Schedule Order"
   - Click "▶️ Start Pipeline"
   - Wait for completion (~30 seconds)

#### Option 2: Via Airflow UI

1. Go to `http://localhost:8080`
2. Find DAG: `supply_chain_pipeline`
3. Click "Trigger DAG" (play button)
4. Monitor progress in Graph view

### Using the Client Portal

1. **Register as Client:**
   - Go to landing page
   - Click "Client Portal" → "Register"
   - Create account

2. **Check Order Status:**
   - Login to client portal
   - Enter order ID or ask: "Where is my order 673e6b6e3c245?"
   - Get AI-powered delivery prediction

### Using the Admin Portal

1. **View Real-Time Data:**
   - Click "📋 View Data"
   - See all shipments with risk classification

2. **Analyze Trends:**
   - Click "📊 Analytics"
   - View categorical breakdowns
   - Identify delay patterns

3. **Monitor Client Activity:**
   - Click "📝 Client Logs"
   - See all client queries
   - Track prediction accuracy

---

##  Project Structure

```
supply-chain-analytics/
│
├── app.py                          # Main Streamlit application
├── requirements.txt                # Python dependencies
├── README.md                       # This file
│
├── configs/
│   ├── .env                        # Environment variables
│   └── google-credentials.json    # GCP service account key
│
├── data/
│   ├── cleaned_supply_chain_data.csv  # Sample data
│   └── users.json                  # User database
│
├── supply-chain-airflow/
│   ├── dags/
│   │   └── supply_chain_pipeline.py   # Main DAG
│   ├── docker-compose.yml          # Airflow services
│   ├── .env                        # Airflow environment
│   └── requirements.txt            # Airflow dependencies
│
├── supply_chain_dbt/               # dbt transformations
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_shipments.sql
│   │   └── marts/
│   │       └── shipment_metrics.sql
│   └── dbt_project.yml
│
├── kafka_producer.py               # Kafka producer API
├── kafka_consumer.py               # Kafka consumer API
├── mongodb_utils.py                # MongoDB utilities
├── main_api.py                     # Main orchestrator API
└── test_connection.py              # Connection testing
```

---

##  Data Flow

### Pipeline Stages

#### 1. **Extract** (Google Sheets → Airflow)
- Airflow reads one row from Google Sheets
- Tracks last processed row index
- Incremental processing (one row per run)

#### 2. **Load to MongoDB** (Airflow → Kafka → MongoDB)
- Transforms flat data to nested JSON structure
- Sends to Kafka topic (optional, for observability)
- Directly inserts to MongoDB
- Sets status: `success`

#### 3. **Load to BigQuery** (MongoDB → BigQuery)
- Reads records with status `success`
- Flattens nested JSON
- Loads to `shipments_raw` table
- Updates status: `loaded_to_bq`

#### 4. **Transform** (BigQuery → dbt → BigQuery)
- Creates `stg_shipments` view (flattened)
- Runs dbt transformations
- Generates `shipment_metrics` with 40+ features
- Loads to `test_table_airflow`
- Updates status: `transformed`

#### 5. **Predict** (BigQuery ML)
- Runs ML model on `test_table_airflow`
- Generates predictions:
  - `DELAYED` (> 0.5 hours)
  - `ON_TIME` (-0.5 to 0.5 hours)
  - `EARLY` (< -0.5 hours)

#### 6. **Append** (test_table_airflow → test_table)
- Appends new predictions to accumulated `test_table`
- Maintains historical data for analytics

### Data Schema

**MongoDB Document:**
```json
{
  "_id": "ObjectId",
  "event_id": "evt_row_123_1234567890",
  "timestamp": "2024-01-01T00:00:00",
  "vehicle_data": {
    "gps_latitude": 0.34,
    "gps_longitude": 0.75,
    "eta_variation_hours": 0.93
  },
  "operational_metrics": { ... },
  "external_factors": { ... },
  "performance_indicators": { ... },
  "temporal_features": { ... },
  "processing_status": "success",
  "ingestion_timestamp": "2024-01-01T12:00:00Z"
}
```

**BigQuery test_table Schema:**
- 40+ feature columns
- Temporal features (hour_of_day, day_of_week, etc.)
- Interaction features (traffic_x_weather, etc.)
- Categorical buckets (traffic_bucket, weather_bucket, etc.)
- Label: `label_delay_hours_capped`

---

##  API Documentation

### Kafka Producer API

**Start Streaming:**
```bash
POST http://localhost:8001/start-streaming
Content-Type: application/json

{
  "csv_file_path": "data/cleaned_supply_chain_data.csv",
  "batch_size": 100,
  "streaming_interval": 1
}
```

### Kafka Consumer API

**Start Consumer:**
```bash
POST http://localhost:8002/start-consumer
```

**Get Status:**
```bash
GET http://localhost:8002/status
```

### MongoDB API

**Get Collection Stats:**
```bash
GET http://localhost:8003/stats
```

**Query Events:**
```bash
GET http://localhost:8003/events?limit=10
```

---

##  Troubleshooting

### Configuration Issues

#### Missing Environment Files

**Problem:** Application fails to start with "File not found" or "Environment variable not set"

**Solution:**
```bash
# Check if files exist
ls -la configs/.env
ls -la supply-chain-airflow/.env
ls -la configs/google-credentials.json

# If missing, copy from templates
cp configs/.env.example configs/.env
cp supply-chain-airflow/.env.example supply-chain-airflow/.env

# Edit and add your credentials
nano configs/.env
```

#### Invalid Credentials Format

**Problem:** "Invalid URI" or "Authentication failed"

**Solution:**
- Check for extra spaces in `.env` files
- Ensure no quotes around values (unless required)
- Verify MongoDB URI format: `mongodb+srv://user:pass@cluster.mongodb.net/...`
- Check Kafka bootstrap server format: `server.region.provider.cloud:9092`

#### Google Credentials Not Found

**Problem:** "Could not load credentials" or "Service account not found"

**Solution:**
```bash
# Verify file exists
ls -la configs/google-credentials.json

# Check if it's valid JSON
cat configs/google-credentials.json | python -m json.tool

# Verify it has required fields
grep -E "type|project_id|private_key" configs/google-credentials.json

# If missing, download from GCP Console:
# IAM & Admin → Service Accounts → Keys → Add Key → Create New Key (JSON)
```

#### Environment Variables Not Loading

**Problem:** Application uses default values instead of your configuration

**Solution:**
```bash
# For Streamlit app
python -c "from dotenv import load_dotenv; import os; load_dotenv('configs/.env'); print(os.getenv('BIGQUERY_PROJECT'))"

# For Airflow, check if .env is mounted
docker exec -it airflow-webserver cat /opt/airflow/.env

# Restart services after changing .env
docker-compose restart
```

---

### Common Issues

#### 1. Airflow DAG Not Appearing

**Problem:** DAG doesn't show up in Airflow UI

**Solution:**
```bash
# Check DAG syntax
docker exec -it airflow-webserver airflow dags list

# Check for errors
docker exec -it airflow-webserver airflow dags list-import-errors
```

#### 2. MongoDB Connection Failed

**Problem:** `InvalidURI: URI must begin with 'mongodb://' or 'mongodb+srv://'`

**Solution:**
- Ensure Extra field in Airflow connection has valid JSON:
  ```json
  {"srv": true}
  ```
- Or use full URI in Extra:
  ```json
  {"uri": "mongodb+srv://user:pass@cluster.mongodb.net/db"}
  ```

#### 3. Kafka Timeout

**Problem:** Messages timing out after 5 minutes

**Solution:**
- Verify Kafka credentials in Airflow Variables
- Check network connectivity to Confluent Cloud
- Reduce timeout in DAG:
  ```python
  'socket.timeout.ms': 5000,
  'message.timeout.ms': 5000
  ```

#### 4. BigQuery ML Prediction Error

**Problem:** `Column not found` or schema mismatch

**Solution:**
- Ensure `test_table_airflow` has same schema as training data
- Check that all feature columns exist
- Verify model was trained with correct features

#### 5. Streamlit App Not Loading Data

**Problem:** Empty dashboard or "No data" message

**Solution:**
```bash
# Check if test_table has data
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `project.supply_chain.test_table`'

# Run pipeline to generate data
# Via Streamlit: Admin Portal → Schedule Order
```

### Debug Mode

Enable detailed logging:

**Airflow:**
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

**Streamlit:**
```bash
streamlit run app.py --logger.level=debug
```

---

##  Testing

### Test Connections

```bash
python test_connection.py
```

This will test:
- Python package imports
- Environment variables
- MongoDB connection
- Kafka connection
- Data file accessibility

### Test Individual Components

**Test MongoDB:**
```bash
python mongodb_utils.py
```

**Test Kafka Producer:**
```bash
python kafka_producer.py
```

**Test Kafka Consumer:**
```bash
python kafka_consumer.py
```

---

## Performance

### Pipeline Metrics

- **Processing Time:** ~30 seconds per row
- **Throughput:** 120 rows/hour (with 30s interval)
- **ML Prediction Latency:** <2 seconds
- **Dashboard Load Time:** <3 seconds

### Optimization Tips

1. **Batch Processing:** Modify DAG to process multiple rows
2. **Parallel Tasks:** Use Airflow task groups
3. **Caching:** Add Redis for frequently accessed data
4. **Indexing:** Create indexes on MongoDB queries
5. **Partitioning:** Partition BigQuery tables by date

---

## Security

### Best Practices Implemented

- Password hashing (SHA-256)
- Environment variables for secrets
- Service account authentication
- SASL_SSL for Kafka
- MongoDB authentication
- API key authentication for Euri AI

### Additional Recommendations

- Use secret managers (Google Secret Manager, AWS Secrets Manager)
- Implement rate limiting on APIs
- Add OAuth2 for user authentication
- Enable audit logging
- Use VPC for network isolation

---

##  Deployment

### Deploy to Google Cloud

**1. Deploy Streamlit App (Cloud Run):**
```bash
gcloud run deploy supply-chain-app \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

**2. Deploy Airflow (Cloud Composer):**
```bash
gcloud composer environments create supply-chain-airflow \
  --location us-central1 \
  --python-version 3.11
```

**3. Upload DAGs:**
```bash
gcloud composer environments storage dags import \
  --environment supply-chain-airflow \
  --location us-central1 \
  --source supply-chain-airflow/dags/
```

### Deploy to AWS

**1. Deploy Streamlit (ECS Fargate):**
- Build Docker image
- Push to ECR
- Create ECS task definition
- Deploy to Fargate

**2. Deploy Airflow (MWAA):**
- Create S3 bucket for DAGs
- Upload DAGs and requirements
- Create MWAA environment

---

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Acknowledgment

- Confluent for Kafka Cloud
- Google Cloud for BigQuery ML
- MongoDB Atlas for database hosting
- Euri AI for natural language processing
- Streamlit for the amazing dashboard framework

---




