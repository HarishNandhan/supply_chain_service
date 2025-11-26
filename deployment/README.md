# 🚀 Automated Deployment Scripts

> **⚡ FASTEST START:** Run `bash deployment/deploy-all.sh` after editing `config.sh`

This folder contains automated scripts to deploy your Supply Chain Management System to Google Cloud Platform in **1 command**.

## 🎯 Quick Links

- 🚀 **New here?** → Read [START_HERE.md](START_HERE.md)
- ⚡ **Want speed?** → Read [QUICKSTART.md](QUICKSTART.md)
- 📊 **Want visuals?** → Read [VISUAL_GUIDE.md](VISUAL_GUIDE.md)
- ✅ **Want checklist?** → Read [CHECKLIST.md](CHECKLIST.md)
- 📚 **Want everything?** → You're in the right place!

## 📁 What's Inside

```
deployment/
├── deploy-all.sh           # 🎯 ONE-CLICK: Run everything automatically
├── 1-setup.sh              # Setup & authentication
├── 2-upload-secrets.sh     # Upload credentials
├── 3-deploy-streamlit.sh   # Deploy dashboard
├── 4-deploy-airflow.sh     # Deploy orchestrator
├── 5-connect-services.sh   # Connect everything
├── config.sh               # Configuration file (EDIT THIS FIRST!)
├── update-app.sh           # Update deployed apps
├── view-logs.sh            # View application logs
└── README.md               # This file
```

## 🎯 Quick Start (Easiest Way)

### Option 1: One-Click Deployment

```bash
# 1. Edit configuration
nano deployment/config.sh  # Update PROJECT_ID and passwords

# 2. Run everything
bash deployment/deploy-all.sh
```

That's it! Wait 30-45 minutes and your app will be live.

### Option 2: Step-by-Step

If you prefer more control:

```bash
# Step 1: Setup
bash deployment/1-setup.sh

# Step 2: Upload secrets
bash deployment/2-upload-secrets.sh

# Step 3: Deploy Streamlit
bash deployment/3-deploy-streamlit.sh

# Step 4: Deploy Airflow
bash deployment/4-deploy-airflow.sh

# Step 5: Connect services
bash deployment/5-connect-services.sh
```

## ⚙️ Configuration

Before running, edit `deployment/config.sh`:

```bash
# REQUIRED: Change these
export PROJECT_ID="your-project-id"
export AIRFLOW_PASSWORD="your-secure-password"

# OPTIONAL: Adjust resources
export STREAMLIT_MEMORY="2Gi"
export STREAMLIT_CPU="2"
```

## 📊 After Deployment

### View Your Apps
```bash
# Check deployment info
cat deployment/deployment-info.txt
```

### View Logs
```bash
bash deployment/view-logs.sh
```

### Update Code
```bash
# After making code changes
bash deployment/update-app.sh
```

## 🔧 Troubleshooting

### Check Service Status
```bash
gcloud run services list
```

### View Recent Logs
```bash
gcloud run services logs read streamlit-app --region us-central1 --limit 50
```

### Restart Service
```bash
gcloud run services update streamlit-app --region us-central1
```

## 💰 Cost Estimate

- Idle: ~$5-10/month
- Active: ~$50-100/month
- Total: ~$60-130/month

## 🆘 Common Issues

### "Permission Denied"
```bash
# Re-authenticate
gcloud auth login
```

### "API Not Enabled"
```bash
# Run setup again
bash deployment/1-setup.sh
```

### "Secret Not Found"
```bash
# Re-upload secrets
bash deployment/2-upload-secrets.sh
```

## 📚 What Each Script Does

### deploy-all.sh
Runs all deployment steps automatically. Best for first-time deployment.

### 1-setup.sh
- Authenticates with Google Cloud
- Enables required APIs
- Validates credentials

### 2-upload-secrets.sh
- Uploads MongoDB URI
- Uploads API keys
- Uploads Kafka credentials
- Sets permissions

### 3-deploy-streamlit.sh
- Builds Streamlit Docker image
- Deploys to Cloud Run
- Configures environment variables

### 4-deploy-airflow.sh
- Creates Cloud SQL database
- Builds Airflow Docker image
- Deploys to Cloud Run

### 5-connect-services.sh
- Connects Streamlit to Airflow
- Displays all URLs
- Saves deployment info

## 🎉 Success!

After deployment, you'll get:
- Streamlit Dashboard URL
- Airflow Webserver URL
- Login credentials
- Deployment summary

Open the URLs in your browser and start using your app!
