# 🔐 Environment Setup Guide

## Required Environment Variables

Before deploying, you need to set up the following environment variables:

### For Dashboard Deployment (Cloud Run)

| Variable | Description | Example |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | Your Google Cloud Project ID | `my-project-123456` |
| `GCP_DATASET_ID` | Your BigQuery dataset ID | `db` |

### For Website Deployment (Vercel)

| Variable | Description | Example |
|----------|-------------|---------|
| `NEXT_PUBLIC_DASHBOARD_URL` | Your deployed dashboard URL | `https://housing-dashboard-xxx.run.app` |

---

## How to Set Environment Variables

### Windows PowerShell

```powershell
# Set for current session
$env:GCP_PROJECT_ID="your-project-id"
$env:GCP_DATASET_ID="your-dataset-id"

# Verify they're set
echo $env:GCP_PROJECT_ID
echo $env:GCP_DATASET_ID

# Then deploy
.\deploy.ps1
```

### Linux/Mac/Git Bash

```bash
# Set for current session
export GCP_PROJECT_ID="your-project-id"
export GCP_DATASET_ID="your-dataset-id"

# Verify they're set
echo $GCP_PROJECT_ID
echo $GCP_DATASET_ID

# Then deploy
./deploy.sh
```

### Using a .env File (Local Development)

```bash
# Create .env file from template
cp .env.example .env

# Edit .env with your values
# Then source it before deploying

# Linux/Mac/Git Bash:
source .env

# PowerShell:
Get-Content .env | ForEach-Object {
    $name, $value = $_.split('=')
    Set-Content env:\$name $value
}
```

---

## Security Notes

✅ **Safe to commit:**
- `.env.example` (template only)
- Deployment scripts (no secrets)

❌ **Never commit:**
- `.env` (contains your actual values)
- Service account JSON files
- API keys

The `.gitignore` is already configured to exclude sensitive files.

---

## For Vercel Deployment

After deploying to Vercel, add environment variables in the dashboard:

1. Go to: https://vercel.com/dashboard
2. Select your project
3. Go to **Settings** → **Environment Variables**
4. Add `NEXT_PUBLIC_DASHBOARD_URL` with your Cloud Run URL
5. Click **Save**
6. Redeploy

---

## Quick Reference

### Deploy Dashboard
```bash
# Set environment variables
export GCP_PROJECT_ID="your-project-id"
export GCP_DATASET_ID="your-dataset-id"

# Deploy
./deploy.sh
```

### Deploy Website
```bash
cd web
vercel
# Then add NEXT_PUBLIC_DASHBOARD_URL in Vercel dashboard
```
