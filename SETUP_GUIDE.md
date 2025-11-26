# 🔧 Complete Setup Guide

> **Step-by-step guide to configure all environment files and credentials**

---

## 📋 Overview

This guide will help you set up all required configuration files and credentials for the Supply Chain Analytics Platform.

**What you'll configure:**
1. ✅ Main application environment (`.env`)
2. ✅ Airflow environment (`.env`)
3. ✅ Airflow variables (`airflow_variables.json`)
4. ✅ Google Cloud credentials (`google-credentials.json`)

**Time required:** ~30 minutes

---

## 🎯 Prerequisites

Before starting, sign up for these services:

- [ ] [MongoDB Atlas](https://www.mongodb.com/cloud/atlas) (Free tier available)
- [ ] [Confluent Kafka](https://confluent.cloud) (Free trial available)
- [ ] [Google Cloud Platform](https://cloud.google.com) (Free tier available)
- [ ] [Euri AI](https://euron.one) (API key required)
- [ ] [Google Sheets](https://sheets.google.com) (Free with Google account)

---

## 📁 Configuration Files Overview

### Files You Need to Create

| File | Template | Purpose |
|------|----------|---------|
| `configs/.env` | `configs/.env.example` | Main app configuration |
| `supply-chain-airflow/.env` | `supply-chain-airflow/.env.example` | Airflow configuration |
| `airflow_variables.json` | `airflow_variables.example.json` | Airflow variables |
| `configs/google-credentials.json` | Download from GCP | GCP authentication |

### Files Already Protected

These files are in `.gitignore` and won't be committed to Git:
- ✅ `configs/.env`
- ✅ `supply-chain-airflow/.env`
- ✅ `airflow_variables.json`
- ✅ `configs/google-credentials.json`
- ✅ `data/users.json`

---

## 🚀 Step-by-Step Setup

### Step 1: Copy Template Files

```bash
# Navigate to project root
cd supply-chain-analytics

# Copy all template files
cp configs/.env.example configs/.env
cp supply-chain-airflow/.env.example supply-chain-airflow/.env
cp airflow_variables.example.json airflow_variables.json

# Verify files were created
ls -la configs/.env
ls -la supply-chain-airflow/.env
ls -la airflow_variables.json
```

---

### Step 2: Set Up MongoDB Atlas

#### 2.1 Create MongoDB Cluster

1. Go to [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
2. Sign up or log in
3. Click **"Build a Database"**
4. Choose **"Free"** tier (M0)
5. Select cloud provider and region
6. Name your cluster: `supply-chain-cluster`
7. Click **"Create"**

#### 2.2 Create Database User

1. Go to **"Database Access"** (left sidebar)
2. Click **"Add New Database User"**
3. Choose **"Password"** authentication
4. Username: `supply_chain_user`
5. Password: Generate a strong password (save it!)
6. Database User Privileges: **"Read and write to any database"**
7. Click **"Add User"**

#### 2.3 Allow Network Access

1. Go to **"Network Access"** (left sidebar)
2. Click **"Add IP Address"**
3. Click **"Allow Access from Anywhere"** (for development)
   - Or add your specific IP for production
4. Click **"Confirm"**

#### 2.4 Get Connection String

1. Go to **"Database"** (left sidebar)
2. Click **"Connect"** on your cluster
3. Choose **"Connect your application"**
4. Copy the connection string
5. Replace `<password>` with your actual password
6. Replace `<dbname>` with `supply_chain_analytics`

**Example:**
```
mongodb+srv://supply_chain_user:YOUR_PASSWORD@supply-chain-cluster.xxxxx.mongodb.net/supply_chain_analytics?retryWrites=true&w=majority
```

#### 2.5 Update Configuration

Add to `configs/.env` and `supply-chain-airflow/.env`:
```bash
MONGODB_URI=mongodb+srv://supply_chain_user:YOUR_PASSWORD@supply-chain-cluster.xxxxx.mongodb.net/supply_chain_analytics?retryWrites=true&w=majority
MONGODB_DATABASE=supply_chain_analytics
MONGODB_COLLECTION=scheduler_table
```

---

### Step 3: Set Up Confluent Kafka

#### 3.1 Create Kafka Cluster

1. Go to [Confluent Cloud](https://confluent.cloud)
2. Sign up or log in
3. Click **"Add cluster"**
4. Choose **"Basic"** (free tier)
5. Select cloud provider and region
6. Name: `supply-chain-kafka`
7. Click **"Launch cluster"**

#### 3.2 Create Kafka Topic

1. Go to **"Topics"** (left sidebar)
2. Click **"Create topic"**
3. Topic name: `supply_chain`
4. Partitions: `1`
5. Click **"Create with defaults"**

#### 3.3 Create API Key

1. Go to **"API Keys"** (left sidebar)
2. Click **"Create key"**
3. Scope: **"Global access"**
4. Click **"Next"**
5. **IMPORTANT:** Copy and save both:
   - API Key
   - API Secret (shown only once!)
6. Click **"Done"**

#### 3.4 Get Bootstrap Server

1. Go to **"Cluster Settings"**
2. Find **"Bootstrap server"**
3. Copy the URL (e.g., `pkc-xxxxx.us-east-2.aws.confluent.cloud:9092`)

#### 3.5 Update Configuration

Add to `configs/.env` and `supply-chain-airflow/.env`:
```bash
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-2.aws.confluent.cloud:9092
KAFKA_API_KEY=YOUR_API_KEY
KAFKA_API_SECRET=YOUR_API_SECRET
KAFKA_TOPIC=supply_chain
```

---

### Step 4: Set Up Google Cloud Platform

#### 4.1 Create GCP Project

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Click project dropdown → **"New Project"**
3. Project name: `supply-chain-project`
4. Click **"Create"**
5. Note your **Project ID** (e.g., `supply-chain-project-123456`)

#### 4.2 Enable Required APIs

```bash
# Using gcloud CLI (or enable via Console)
gcloud services enable bigquery.googleapis.com
gcloud services enable sheets.googleapis.com
gcloud services enable secretmanager.googleapis.com
```

Or via Console:
1. Go to **"APIs & Services"** → **"Enable APIs and Services"**
2. Search and enable:
   - BigQuery API
   - Google Sheets API
   - Secret Manager API

#### 4.3 Create Service Account

1. Go to **"IAM & Admin"** → **"Service Accounts"**
2. Click **"Create Service Account"**
3. Name: `airflow-supply-chain`
4. Description: `Service account for Supply Chain Analytics`
5. Click **"Create and Continue"**

#### 4.4 Grant Roles

Add these roles:
- **BigQuery Admin**
- **BigQuery Data Editor**
- **BigQuery Job User**
- **Service Account User**

Click **"Continue"** → **"Done"**

#### 4.5 Create and Download Key

1. Click on the service account you just created
2. Go to **"Keys"** tab
3. Click **"Add Key"** → **"Create new key"**
4. Choose **"JSON"**
5. Click **"Create"**
6. File will download automatically (e.g., `supply-chain-project-123456-xxxxx.json`)

#### 4.6 Save Credentials File

```bash
# Create directories if they don't exist
mkdir -p configs
mkdir -p supply-chain-airflow/configs

# Move downloaded file (replace filename with yours)
mv ~/Downloads/supply-chain-project-*.json configs/google-credentials.json

# Copy to Airflow directory
cp configs/google-credentials.json supply-chain-airflow/configs/google-credentials.json

# Verify file is valid JSON
cat configs/google-credentials.json | python -m json.tool
```

#### 4.7 Create BigQuery Dataset

```bash
# Using bq CLI
bq mk --dataset --location=US supply_chain

# Or via Console:
# BigQuery → Create Dataset → Dataset ID: supply_chain → Location: US
```

#### 4.8 Update Configuration

Add to `configs/.env` and `supply-chain-airflow/.env`:
```bash
BIGQUERY_PROJECT=supply-chain-project-123456
BIGQUERY_DATASET=supply_chain
BIGQUERY_TABLE=shipments_raw
BQ_MODEL=delay_regressor_v6
BQ_TEST_TBL=test_table
```

For Airflow only (`supply-chain-airflow/.env`):
```bash
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/configs/google-credentials.json
```

---

### Step 5: Set Up Google Sheets

#### 5.1 Create Google Sheet

1. Go to [Google Sheets](https://sheets.google.com)
2. Create a new spreadsheet
3. Name it: `Supply Chain Data`
4. Add your data or use sample data from `data/cleaned_supply_chain_data.csv`

#### 5.2 Share with Service Account

1. Click **"Share"** button
2. Add your service account email (from `google-credentials.json`)
   - Example: `airflow-supply-chain@supply-chain-project-123456.iam.gserviceaccount.com`
3. Give **"Editor"** access
4. Uncheck **"Notify people"**
5. Click **"Share"**

#### 5.3 Get Sheet ID

From the URL:
```
https://docs.google.com/spreadsheets/d/[SHEET_ID]/edit
```

Copy the `SHEET_ID` part.

#### 5.4 Update Configuration

Add to `supply-chain-airflow/.env`:
```bash
GOOGLE_SHEET_ID=YOUR_SHEET_ID
```

---

### Step 6: Set Up Euri AI

#### 6.1 Get API Key

1. Go to [Euri AI](https://euron.one)
2. Sign up or log in
3. Go to **"API Keys"** or **"Settings"**
4. Click **"Create New Key"**
5. Copy the API key (starts with `euri-`)

#### 6.2 Update Configuration

Add to `configs/.env`:
```bash
EURI_API_KEY=euri-your-api-key-here
EURI_MODEL_NAME=gpt-4.1-nano
```

---

### Step 7: Configure Airflow Variables

Edit `airflow_variables.json` with all your credentials:

```json
{
  "google_sheet_id": "YOUR_SHEET_ID",
  "kafka_bootstrap_servers": "pkc-xxxxx.us-east-2.aws.confluent.cloud:9092",
  "kafka_api_key": "YOUR_KAFKA_API_KEY",
  "kafka_api_secret": "YOUR_KAFKA_API_SECRET",
  "kafka_topic": "supply_chain",
  "mongodb_uri": "mongodb+srv://user:pass@cluster.mongodb.net/...",
  "mongodb_database": "supply_chain_analytics",
  "mongodb_collection": "scheduler_table",
  "bigquery_project": "supply-chain-project-123456",
  "bigquery_dataset": "supply_chain",
  "bigquery_table": "shipments_raw",
  "bq_model": "delay_regressor_v6",
  "bq_test_tbl": "test_table",
  "ml_model_name": "delay_regressor_v6",
  "last_processed_row_index": "0"
}
```

---

## ✅ Verification

### Check Configuration Files

```bash
# Check if all files exist
ls -la configs/.env
ls -la supply-chain-airflow/.env
ls -la airflow_variables.json
ls -la configs/google-credentials.json

# Verify .env files have content
wc -l configs/.env
wc -l supply-chain-airflow/.env

# Verify JSON files are valid
cat airflow_variables.json | python -m json.tool
cat configs/google-credentials.json | python -m json.tool
```

### Test Connections

```bash
# Test MongoDB connection
python test_connection.py

# Test all connections
python -c "
from dotenv import load_dotenv
import os
load_dotenv('configs/.env')
print('MongoDB URI:', os.getenv('MONGODB_URI')[:20] + '...')
print('Kafka Server:', os.getenv('KAFKA_BOOTSTRAP_SERVERS'))
print('BigQuery Project:', os.getenv('BIGQUERY_PROJECT'))
print('Euri API Key:', os.getenv('EURI_API_KEY')[:10] + '...')
"
```

---

## 🔒 Security Checklist

Before proceeding:

- [ ] All `.env` files created and filled
- [ ] `google-credentials.json` downloaded and saved
- [ ] `airflow_variables.json` updated with credentials
- [ ] All files are in `.gitignore`
- [ ] No credentials committed to Git
- [ ] Credentials stored securely (password manager)
- [ ] Network access configured (MongoDB, Kafka)
- [ ] Service account has required permissions

---

## 🚀 Next Steps

After completing this setup:

1. **Start Airflow:**
   ```bash
   cd supply-chain-airflow
   docker-compose up -d
   ```

2. **Upload Airflow Variables:**
   - Go to http://localhost:8080
   - Admin → Variables → Import Variables
   - Upload `airflow_variables.json`

3. **Run the Application:**
   ```bash
   streamlit run app.py
   ```

4. **Test the Pipeline:**
   - Go to Streamlit app
   - Login as admin (admin/admin123)
   - Click "Schedule Order"
   - Trigger pipeline

---

## 📞 Need Help?

**Common Issues:**
- See [README.md](README.md#troubleshooting) for troubleshooting
- See [SECURITY.md](SECURITY.md) for security best practices
- See [QUICKSTART.md](QUICKSTART.md) for quick setup

**Support:**
- 📧 Email: support@yourcompany.com
- 🐛 Issues: [GitHub Issues](your-repo/issues)
- 💬 Chat: [Discord/Slack](your-chat-url)

---

**Setup complete! You're ready to run the platform! 🎉**
