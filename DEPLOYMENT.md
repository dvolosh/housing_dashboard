# 🚀 Deploying Housing Dashboard to Google Cloud Run

This guide walks you through deploying your Dash application to Google Cloud Run.

## 📋 Prerequisites

### 1. Google Cloud SDK (gcloud CLI)
**Windows Installation:**
```powershell
# Download and run the installer from:
# https://cloud.google.com/sdk/docs/install

# After installation, restart your terminal and verify:
gcloud --version
```

### 2. Authenticate with Google Cloud
```powershell
# Login to your Google account
gcloud auth login

# Set your project
gcloud config set project vant-486316
```

### 3. Enable Billing
- Go to [Google Cloud Console](https://console.cloud.google.com/)
- Ensure billing is enabled for project `vant-486316`
- Cloud Run has a generous free tier: 2M requests/month

---

## 🎯 Quick Deployment (One Command)

From your project root directory:

```powershell
# Windows PowerShell
.\deploy.ps1
```

```bash
# Linux/Mac or Git Bash on Windows
./deploy.sh
```

That's it! The script will:
1. ✅ Build your Docker image
2. ✅ Push it to Google Container Registry
3. ✅ Deploy to Cloud Run
4. ✅ Give you a public URL

---

## 🔧 Manual Deployment (Step-by-Step)

If you prefer to run commands manually:

### Step 1: Enable Required APIs
```powershell
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable artifactregistry.googleapis.com
```

### Step 2: Build Docker Image
```powershell
# Build and push to Google Container Registry
gcloud builds submit --tag gcr.io/vant-486316/housing-dashboard
```

### Step 3: Deploy to Cloud Run
```powershell
gcloud run deploy housing-dashboard `
  --image gcr.io/vant-486316/housing-dashboard `
  --platform managed `
  --region us-central1 `
  --allow-unauthenticated `
  --memory 1Gi `
  --cpu 1 `
  --timeout 300 `
  --max-instances 10 `
  --set-env-vars "GCP_PROJECT_ID=vant-486316,GCP_DATASET_ID=db"
```

### Step 4: Get Your URL
```powershell
gcloud run services describe housing-dashboard --region us-central1 --format='value(status.url)'
```

---

## 🔐 Security & Authentication

### BigQuery Access
Your Cloud Run service automatically has access to BigQuery via:
- **Default Compute Service Account** (automatically configured)
- No additional authentication needed if deploying to the same GCP project

### Making the Dashboard Private (Optional)
If you want to restrict access:

```powershell
# Remove public access
gcloud run services remove-iam-policy-binding housing-dashboard `
  --region=us-central1 `
  --member="allUsers" `
  --role="roles/run.invoker"

# Add specific user
gcloud run services add-iam-policy-binding housing-dashboard `
  --region=us-central1 `
  --member="user:your-email@gmail.com" `
  --role="roles/run.invoker"
```

---

## 💰 Cost Estimates

Cloud Run pricing (as of 2026):
- **Free Tier:** 2M requests/month, 360,000 GB-seconds, 180,000 vCPU-seconds
- **Your Configuration:** 1Gi memory, 1 CPU
- **Estimated Cost:** $0-5/month for typical resume project traffic

### Cost Optimization Tips
1. Cloud Run charges only when requests are being processed
2. No charges when idle (unlike EC2 or always-on servers)
3. Auto-scales to zero when not in use

---

## 📊 Monitoring & Logs

### View Real-Time Logs
```powershell
gcloud run logs tail housing-dashboard --region us-central1
```

### View in Console
- Go to: https://console.cloud.google.com/run
- Select `housing-dashboard` service
- Click **LOGS** tab

---

## 🔄 Updating Your Dashboard

After making code changes:

```powershell
# Redeploy with one command
.\deploy.ps1
```

Cloud Run will automatically:
1. Build new image
2. Deploy with zero downtime
3. Route traffic to new version

---

## 🐛 Troubleshooting

### Build Fails
```powershell
# Check if all files are present
ls Dockerfile
ls dashboard/dash_app.py
ls dashboard/requirements.txt

# Try building locally first
docker build -t housing-dashboard .
```

### Deployment Succeeds but Dashboard Doesn't Load
```powershell
# Check logs for errors
gcloud run logs tail housing-dashboard --region us-central1

# Common issues:
# 1. Port mismatch (Cloud Run expects $PORT env var - already configured)
# 2. BigQuery permissions (should auto-configure)
# 3. Missing environment variables
```

### Memory/Performance Issues
```powershell
# Increase memory to 2Gi
gcloud run services update housing-dashboard `
  --region us-central1 `
  --memory 2Gi
```

---

## 🎓 Next Steps for Your Resume Project

### 1. Custom Domain (Optional)
```powershell
gcloud run domain-mappings create --service housing-dashboard --domain your-domain.com
```

### 2. Add to Resume
```
Housing Market Dashboard
- Live URL: https://housing-dashboard-xxx-uc.a.run.app
- Stack: Python, Dash, BigQuery, Vertex AI, Docker, Cloud Run
- Automated CI/CD pipeline with Cloud Build
```

### 3. Future Enhancements
- [ ] Add Cloud Scheduler to refresh data automatically
- [ ] Integrate Cloud Functions for Reddit scraping
- [ ] Connect Vertex AI for sentiment analysis
- [ ] Set up Cloud Monitoring alerts

---

## 📚 Additional Resources

- [Cloud Run Documentation](https://cloud.google.com/run/docs)
- [Cloud Run Pricing](https://cloud.google.com/run/pricing)
- [Dash Deployment Guide](https://dash.plotly.com/deployment)
- [BigQuery Python Client](https://cloud.google.com/python/docs/reference/bigquery/latest)

---

## 🎉 Success!

Your dashboard should now be live! Share the URL on your resume and GitHub README.

Need help? Check the logs or open an issue in your repo.
