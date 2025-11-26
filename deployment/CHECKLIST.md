# ✅ Deployment Checklist

## Before You Start

- [ ] Google Cloud Project created
- [ ] Billing enabled on project
- [ ] Know your Project ID
- [ ] Have terminal/command prompt access
- [ ] Files exist:
  - [ ] `configs/.env`
  - [ ] `configs/google-credentials.json`

## Deployment Steps

### Automated Way (Recommended)

- [ ] **Step 1:** Edit `deployment/config.sh`
  - [ ] Update `PROJECT_ID`
  - [ ] Update `AIRFLOW_PASSWORD` (optional)
  
- [ ] **Step 2:** Run `bash deployment/deploy-all.sh`

- [ ] **Step 3:** Wait 30-45 minutes ☕

- [ ] **Step 4:** Save the URLs provided

### Manual Way (Optional)

- [ ] Run `bash deployment/1-setup.sh`
- [ ] Run `bash deployment/2-upload-secrets.sh`
- [ ] Run `bash deployment/3-deploy-streamlit.sh`
- [ ] Run `bash deployment/4-deploy-airflow.sh`
- [ ] Run `bash deployment/5-connect-services.sh`

## After Deployment

- [ ] Open Streamlit URL in browser
- [ ] Test login with admin credentials
- [ ] Open Airflow URL in browser
- [ ] Verify DAG is visible
- [ ] Test triggering pipeline
- [ ] Check logs: `bash deployment/view-logs.sh`
- [ ] Save deployment info from `deployment/deployment-info.txt`

## Security (Important!)

- [ ] Change default passwords in `config.sh`
- [ ] Review who has access to your GCP project
- [ ] Enable 2FA on your Google account
- [ ] Don't commit secrets to git

## Optional Enhancements

- [ ] Set up custom domain
- [ ] Configure monitoring alerts
- [ ] Set up automated backups
- [ ] Enable authentication for public access
- [ ] Set up CI/CD pipeline

## Troubleshooting

If something goes wrong:

- [ ] Check logs: `bash deployment/view-logs.sh`
- [ ] Verify services: `gcloud run services list`
- [ ] Check secrets: `gcloud secrets list`
- [ ] Re-run failed step individually

## Success Indicators

You know it worked when:

- ✅ You can access Streamlit dashboard
- ✅ You can login successfully
- ✅ You can access Airflow webserver
- ✅ DAG appears in Airflow UI
- ✅ No errors in logs
- ✅ Pipeline can be triggered

## Cost Monitoring

- [ ] Set up billing alerts in GCP Console
- [ ] Monitor usage weekly
- [ ] Review costs monthly
- [ ] Adjust resources if needed

---

**Estimated Time:** 2 minutes (your time) + 30 minutes (automated)

**Estimated Cost:** $60-130/month

**Difficulty:** Easy ⭐

---

Print this checklist and check off items as you go! 📋
