# 🚀 Quick Start Guide

> **Get the Supply Chain Analytics Platform running in 30 minutes**

This guide will help you set up and run the entire platform from scratch, even if you're new to these technologies.

---

## 📋 What You'll Build

By the end of this guide, you'll have:
- ✅ A working Airflow pipeline that processes supply chain data
- ✅ ML predictions for delivery delays
- ✅ An interactive dashboard for admins and clients
- ✅ AI-powered chat for order status

---

## ⏱️ Time Required

- **Minimum Setup:** 15 minutes (local only, no cloud)
- **Full Setup:** 30 minutes (with cloud services)

---

## 🎯 Step-by-Step Setup

### Step 1: Prerequisites (5 minutes)

**Install Required Software:**

1. **Python 3.11+**
   ```bash
   # Check if installed
   python --version
   
   # If not, download from python.org
   ```

2. **Docker Desktop**
   ```bash
   # Download from docker.com
   # Start Docker Desktop after installation
   ```

3. **Git**
   ```bash
   # Check if installed
   git --version
   
   # If not, download from git-scm.com
   ```

---

### Step 2: Clone and Install (3 minutes)

```bash
# Clone the repository
git clone <your-repo-url>
cd supply-chain-analytics

# Install Python dependencies
pip install -r requirements.txt

# Install Airflow dependencies
cd supply-chain-airflow
pip install -r requirements.txt
cd ..
```

---

### Step 3: Set Up Cloud Services (10 minutes)

#### A. Google Cloud Platform

1. **Create a Project:**
   - Go to [console.cloud.google.com](https://console.cloud.google.com)
   - Click "New Project"
   - Name it: `supply-chain-project`
   - Note your Project ID

2. **Enable APIs:**
   - Go to "APIs & Services" → "Enable APIs and Services"
   - Search and enable:
     - BigQuery API
     - Google Sheets API

3. **Create Service Account:**
   - Go to "IAM & Admin" → "Service Accounts"
   - Click "Create Service Account"
   - Name: `airflow-supply-chain`
   - Grant roles:
     - BigQuery Admin
     - BigQuery Data Editor
   - Click "Create Key" → JSON
   - Save as `configs/google-credentials.json`

4. **Create BigQuery Dataset:**
   - Go to BigQuery console
   - Click your project
   - Click "Create Dataset"
   - Dataset ID: `supply_chain`
   - Location: `US`

#### B. MongoDB Atlas

1. **Create Account:**
   - Go to [mongodb.com/cloud/atlas](https://www.mongodb.com/cloud/atlas)
   - Sign up for free

2. **Create Cluster:**
   - Click "Build a Database"
   - Choose "Free" tier (M0)
   - Select region closest to you
   - Name: `cluster-supply-chain`

3. **Create Database User:**
   - Go to "Database Access"
   - Click "Add New Database User"
   - Username: `supply_chain_user`
   - Password: (generate strong password)
   - Save credentials!

4. **Allow Network Access:**
   - Go to "Network Access"
   - Click "Add IP Address"
   - Click "Allow Access from Anywhere" (for testing)
   - Confirm

5. **Get Connection String:**
   - Go to "Database" → "Connect"
   - Choose "Connect your application"
   - Copy the connection string
   - Replace `<password>` with your password

#### C. Confluent Kafka

1. **Create Account:**
   - Go to [confluent.cloud](https://confluent.cloud)
   - Sign up for free trial

2. **Create Cluster:**
   - Click "Create Cluster"
   - Choose "Basic" (free tier)
   - Select region
   - Name: `supply-chain-kafka`

3. **Create Topic:**
   - Go to "Topics"
   - Click "Create Topic"
   - Name: `supply_chain`
   - Partitions: 1
   - Create

4. **Create API Key:**
   - Go to "API Keys"
   - Click "Create Key"
   - Scope: Global access
   - Save API Key and Secret!

5. **Get Bootstrap Server:**
   - Go to "Cluster Settings"
   - Copy "Bootstrap server" URL

#### D. Euri AI

1. **Get API Key:**
   - Go to [euron.one](https://euron.one)
   - Sign up
   - Go to API Keys
   - Create new key
   - Save it!

#### E. Google Sheets

1. **Create Sheet:**
   - Go to [sheets.google.com](https://sheets.google.com)
   - Create new spreadsheet
   - Name: `Supply Chain Data`

2. **Add Sample Data:**
   - Copy data from `data/cleaned_supply_chain_data.csv`
   - Paste into Sheet
   - Ensure first row has column headers

3. **Share with Service Account:**
   - Click "Share"
   - Add your service account email (from step 3A)
   - Give "Editor" access

4. **Get Sheet ID:**
   - Copy from URL: `https://docs.google.com/spreadsheets/d/[SHEET_ID]/edit`

---

### Step 4: Configure Environment (5 minutes)

#### A. Main App Configuration

Create `configs/.env`:

```bash
# BigQuery
BIGQUERY_PROJECT=your-project-id
BIGQUERY_DATASET=supply_chain
BIGQUERY_TABLE_RAW=shipments_raw
BQ_MODEL=delay_regressor_v6
BQ_TEST_TBL=test_table

# Euri AI
EURI_API_KEY=your-euri-api-key
EURI_MODEL_NAME=gpt-4.1-nano

# Airflow (for triggering from Streamlit)
AIRFLOW_URL=http://localhost:8080
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_DAG_ID=supply_chain_pipeline
```

#### B. Airflow Configuration

Create `supply-chain-airflow/.env`:

```bash
# MongoDB
MONGODB_URI=mongodb+srv://user:password@cluster.mongodb.net/?retryWrites=true&w=majority
MONGODB_DATABASE=supply_chain_analytics
MONGODB_COLLECTION=scheduler_table

# Kafka
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-2.aws.confluent.cloud:9092
KAFKA_API_KEY=your-kafka-api-key
KAFKA_API_SECRET=your-kafka-api-secret
KAFKA_TOPIC=supply_chain

# BigQuery
BIGQUERY_PROJECT=your-project-id
BIGQUERY_DATASET=supply_chain
BIGQUERY_TABLE=shipments_raw
BQ_MODEL=delay_regressor_v6
BQ_TEST_TBL=test_table

# Google Cloud
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/configs/google-credentials.json

# Airflow
AIRFLOW_UID=50000
POSTGRES_PORT=5433
```

#### C. Copy Google Credentials

```bash
# Copy to Airflow directory
cp configs/google-credentials.json supply-chain-airflow/configs/
```

---

### Step 5: Start Airflow (3 minutes)

```bash
cd supply-chain-airflow

# Initialize Airflow database (first time only)
docker-compose up airflow-init

# Start all Airflow services
docker-compose up -d

# Check if services are running
docker-compose ps
```

**Expected Output:**
```
NAME                    STATUS
airflow-scheduler       running
airflow-webserver       running
airflow-worker          running
postgres                running
redis                   running
```

**Access Airflow UI:**
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

---

### Step 6: Configure Airflow (5 minutes)

#### A. Upload Variables

1. Go to Airflow UI → **Admin** → **Variables**
2. Click **Choose File**
3. Select `airflow_variables.json` (create it first):

```json
{
  "google_sheet_id": "your-sheet-id",
  "kafka_bootstrap_servers": "your-kafka-bootstrap-server",
  "kafka_api_key": "your-kafka-api-key",
  "kafka_api_secret": "your-kafka-api-secret",
  "kafka_topic": "supply_chain",
  "mongodb_uri": "your-mongodb-uri",
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

4. Click **Import Variables**

#### B. Create Connections

**MongoDB Connection:**
1. Go to **Admin** → **Connections**
2. Click **+** (Add)
3. Fill in:
   - Connection Id: `mongodb_default`
   - Connection Type: `MongoDB`
   - Host: `your-cluster.mongodb.net`
   - Schema: `supply_chain_analytics`
   - Login: `your-username`
   - Password: `your-password`
   - Extra: `{"srv": true}`
4. Click **Test** → **Save**

**BigQuery Connection:**
1. Click **+** (Add)
2. Fill in:
   - Connection Id: `bigquery_default`
   - Connection Type: `Google Cloud`
   - Project Id: `your-project-id`
   - Keyfile Path: `/opt/airflow/configs/google-credentials.json`
3. Click **Test** → **Save**

---

### Step 7: Create BigQuery Tables (2 minutes)

Run these SQL queries in BigQuery console:

```sql
-- 1. Create shipments_raw table (auto-created by Airflow, but you can pre-create)
CREATE TABLE IF NOT EXISTS `your-project.supply_chain.shipments_raw` (
  _id STRING,
  event_id STRING,
  timestamp TIMESTAMP,
  vehicle_data_gps_latitude FLOAT64,
  vehicle_data_gps_longitude FLOAT64,
  -- ... other columns will be auto-detected
);

-- 2. Create test_table (for accumulated predictions)
CREATE TABLE IF NOT EXISTS `your-project.supply_chain.test_table` (
  _id STRING,
  event_id STRING,
  timestamp TIMESTAMP,
  -- ... feature columns will be created by dbt
);

-- 3. Create stg_shipments view (will be created by Airflow)
-- 4. Create shipment_metrics view (will be created by dbt)
```

---

### Step 8: Train ML Model (5 minutes)

**Option 1: Use Pre-trained Model (Recommended)**

If you have existing data in `test_table`:

```sql
CREATE OR REPLACE MODEL `your-project.supply_chain.delay_regressor_v6`
OPTIONS(
  model_type='LINEAR_REG',
  input_label_cols=['label_delay_hours_capped']
) AS
SELECT * EXCEPT(timestamp, _id, event_id, label_delay_hours_raw)
FROM `your-project.supply_chain.shipment_metrics`
WHERE label_delay_hours_capped IS NOT NULL;
```

**Option 2: Run Pipeline First**

Skip this step and let the pipeline create sample data first, then train the model.

---

### Step 9: Start Streamlit App (1 minute)

```bash
# From project root
streamlit run app.py
```

**Access Dashboard:**
- URL: http://localhost:8501

---

### Step 10: Test the Pipeline (2 minutes)

#### Option 1: Via Streamlit (Easiest)

1. Go to http://localhost:8501
2. Click **Admin Portal** → **Login**
   - Username: `admin`
   - Password: `admin123`
3. Click **🚀 Schedule Order**
4. Click **▶️ Start Pipeline**
5. Wait ~30 seconds
6. See prediction results!

#### Option 2: Via Airflow UI

1. Go to http://localhost:8080
2. Find DAG: `supply_chain_pipeline`
3. Toggle it **ON** (if paused)
4. Click **▶** (Trigger DAG)
5. Watch the graph view
6. All tasks should turn green ✅

---

## ✅ Verification Checklist

After setup, verify everything works:

- [ ] Airflow UI accessible at http://localhost:8080
- [ ] Streamlit app accessible at http://localhost:8501
- [ ] Can login to admin portal
- [ ] Can trigger pipeline from Streamlit
- [ ] Pipeline completes successfully
- [ ] Can see predictions in dashboard
- [ ] Can check order status in client portal
- [ ] AI chat responds to queries

---

## 🐛 Common Issues

### Issue 1: Docker Not Starting

**Error:** `Cannot connect to Docker daemon`

**Solution:**
```bash
# Start Docker Desktop
# Wait for it to fully start
# Try again
docker-compose up -d
```

### Issue 2: Airflow DAG Not Appearing

**Error:** DAG not visible in UI

**Solution:**
```bash
# Check DAG syntax
docker exec -it airflow-webserver airflow dags list

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Issue 3: MongoDB Connection Failed

**Error:** `InvalidURI` or connection timeout

**Solution:**
- Check MongoDB Atlas network access (allow your IP)
- Verify connection string format
- Test connection in Airflow UI

### Issue 4: BigQuery Permission Denied

**Error:** `403 Forbidden`

**Solution:**
- Verify service account has BigQuery Admin role
- Check project ID is correct
- Ensure APIs are enabled

### Issue 5: Kafka Timeout

**Error:** `Message timed out`

**Solution:**
- Verify Kafka credentials
- Check bootstrap server URL
- Ensure topic exists
- Network connectivity to Confluent Cloud

---

## 🎉 Success!

If you've made it here, congratulations! You now have a fully functional supply chain analytics platform.

### Next Steps

1. **Add More Data:**
   - Add rows to your Google Sheet
   - Run pipeline multiple times
   - Build up historical data

2. **Explore Features:**
   - Try client portal
   - Check analytics dashboard
   - View client activity logs

3. **Customize:**
   - Modify ML model features
   - Add new visualizations
   - Customize AI responses

4. **Deploy to Production:**
   - Follow deployment guide in README
   - Set up monitoring
   - Configure alerts

---

## 📚 Learn More

- [Full Documentation](README.md)
- [Architecture Deep Dive](ARCHITECTURE.md)
- [API Reference](API.md)
- [Troubleshooting Guide](TROUBLESHOOTING.md)

---

## 💬 Need Help?

- 📧 Email: your-email@example.com
- 🐛 Issues: [GitHub Issues](your-repo-url/issues)
- 💬 Chat: [Discord/Slack](your-chat-url)

---

**Happy Building! 🚀**
