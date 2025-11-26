# 🚀 Supply Chain Airflow Pipeline

Complete Airflow orchestration for the Supply Chain Analytics pipeline.

---

## 📁 Project Structure

```
supply-chain-airflow/
├── dags/
│   └── supply_chain_pipeline.py    # Main DAG definition
├── include/                         # Additional Python modules
├── plugins/                         # Custom Airflow plugins
├── tests/                          # DAG tests
├── Dockerfile                      # Custom Airflow image
├── requirements.txt                # Python dependencies
├── airflow_settings.yaml          # Connections & Variables
├── .env                           # Environment variables
├── FINAL_CHECKLIST.md            # ✅ START HERE!
├── QUICK_START.md                # Quick reference
├── SETUP_GUIDE.md                # Detailed setup guide
└── CONFIG_TEMPLATE.md            # Configuration help
```

---

## ⚡ Quick Start (3 Steps)

### 1. Get Google Sheet ID
- Convert your CSV to Google Sheets
- Copy the Sheet ID from URL
- Share with service account email

### 2. Update Configuration
Edit `airflow_settings.yaml` line 37:
```yaml
variable_value: YOUR_SHEET_ID_HERE
```

Edit `.env` line 14:
```bash
GOOGLE_SHEET_ID=YOUR_SHEET_ID_HERE
```

### 3. Start Airflow
```bash
cd supply-chain-airflow
astro dev start
```

Open: http://localhost:8080(admin/admin)

---

## 📚 Documentation

- **FINAL_CHECKLIST.md** - Complete pre-flight checklist ✅
- **QUICK_START.md** - Fast setup guide ⚡
- **SETUP_GUIDE.md** - Detailed instructions 📖
- **CONFIG_TEMPLATE.md** - Configuration help 🔧

---

## 🔄 Pipeline Overview

**DAG:** `supply_chain_pipeline`

**Flow:**
```
Extract from Google Sheets
    ↓
Load to MongoDB Atlas
    ↓
Transform with dbt
    ↓
Load to BigQuery
    ↓
Run ML Predictions
    ↓
Notify Completion
```

**Trigger:** Manual (from Streamlit or Airflow UI)

---

## 🛠️ Common Commands

```bash
# Start Airflow
astro dev start

# Check status
astro dev ps

# View logs
astro dev logs

# Access container
astro dev bash

# Restart (after config changes)
astro dev restart

# Stop Airflow
astro dev stop
```

---

## 🔗 Key Endpoints

- **Airflow UI:** http://localhost:8080
- **Airflow API:** http://localhost:8080/api/v1
- **Health Check:** http://localhost:8080/health

---

## ⚙️ Configuration

### Already Configured ✅
- GCP Project: `supply-chain-project-476405`
- MongoDB Atlas connection
- BigQuery dataset: `supply_chain`
- Service account credentials

### You Need to Configure 📝
- Google Sheet ID (in 2 files)
- Share Google Sheet with service account

---

## 🎯 What This Pipeline Does

1. **Extracts** shipment data from Google Sheets row by row
2. **Loads** data into MongoDB Atlas `scheduler_table`
3. **Transforms** data using dbt models
4. **Loads** transformed data to BigQuery
5. **Predicts** delays using ML model `delay_regressor_v6`
6. **Logs** completion status and prediction summary

---

## 📊 Monitoring

- **Task Status:** Check Graph view in Airflow UI
- **Logs:** Click on any task to see detailed logs
- **XCom:** View data passed between tasks
- **Variables:** Admin → Variables
- **Connections:** Admin → Connections

---

## 🐛 Troubleshooting

See `SETUP_GUIDE.md` for detailed troubleshooting steps.

**Common Issues:**
- Google Sheet access → Check service account sharing
- MongoDB connection → Verify credentials in `airflow_settings.yaml`
- dbt errors → Check dbt profiles configuration
- BigQuery permissions → Verify service account roles

---

## 🔐 Security Notes

- Service account credentials are mounted at runtime
- MongoDB credentials are in `airflow_settings.yaml` (local dev only)
- Never commit `.env` or credentials to git
- Use Airflow Secrets Backend for production

---

## 📈 Next Steps

1. Complete `FINAL_CHECKLIST.md`
2. Run `astro dev start`
3. Access Airflow UI
4. Trigger the pipeline
5. Monitor execution
6. Check BigQuery for results

---

## 🤝 Integration

This Airflow pipeline integrates with:
- **Streamlit Dashboard** - Triggers pipeline via API
- **MongoDB Atlas** - Stores raw data
- **dbt** - Transforms data
- **BigQuery** - Data warehouse & ML
- **Google Sheets** - Data source

---

## 📞 Support

- Check documentation files in this directory
- Review Airflow logs: `astro dev logs`
- Access container: `astro dev bash`
- Astro CLI docs: https://docs.astronomer.io

---

**Version:** 1.0.0
**Astro Runtime:** 12.1.1
**Airflow Version:** 2.10.x
