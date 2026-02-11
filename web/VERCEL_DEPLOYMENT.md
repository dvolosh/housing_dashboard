# 🚀 Deploying Your Website to Vercel

This guide walks you through deploying your Next.js website to Vercel and configuring the dashboard integration.

## 📋 Prerequisites

1. **Vercel Account** (free)
   - Sign up at: https://vercel.com/signup
   - Connect your GitHub account (recommended)

2. **Dashboard URL**
   - ✅ Already deployed: https://housing-dashboard-wo7crn5xwa-uc.a.run.app

---

## 🎯 Quick Deployment (Recommended)

### Option 1: Deploy via Vercel CLI

```bash
# Navigate to web folder
cd web

# Install Vercel CLI globally
npm install -g vercel

# Login to Vercel
vercel login

# Deploy (follow prompts)
vercel

# For production deployment
vercel --prod
```

### Option 2: Deploy via GitHub (Continuous Deployment)

1. **Push to GitHub:**
   ```bash
   git add .
   git commit -m "Add dashboard integration"
   git push origin main
   ```

2. **Import to Vercel:**
   - Go to: https://vercel.com/new
   - Click "Import Git Repository"
   - Select your `housing_dashboard` repository
   - **Root Directory:** Set to `web`
   - Click "Deploy"

---

## ⚙️ Configure Environment Variables

After deployment, add the dashboard URL:

### Via Vercel Dashboard

1. Go to your project: https://vercel.com/dashboard
2. Click on your project
3. Go to **Settings** → **Environment Variables**
4. Add:
   - **Name:** `NEXT_PUBLIC_DASHBOARD_URL`
   - **Value:** `https://housing-dashboard-wo7crn5xwa-uc.a.run.app`
   - **Environment:** Production, Preview, Development (select all)
5. Click **Save**
6. **Redeploy** (go to Deployments tab → click ⋯ → Redeploy)

### Via CLI

```bash
vercel env add NEXT_PUBLIC_DASHBOARD_URL
# When prompted, paste: https://housing-dashboard-wo7crn5xwa-uc.a.run.app
# Select: Production, Preview, Development

# Redeploy
vercel --prod
```

---

## ✅ Verify Deployment

1. Visit your Vercel URL (provided after deployment)
2. Scroll to "Ready to dive in?" section
3. Click **"Launch Dashboard →"** button
4. Verify it opens: https://housing-dashboard-wo7crn5xwa-uc.a.run.app

---

## 🌐 Custom Domain (Optional)

### Add Your Domain

1. Go to **Settings** → **Domains**
2. Add your domain (e.g., `yourname.com`)
3. Follow DNS configuration instructions
4. SSL automatically configured

### Recommended Setup
- `yourname.com` → Website (Vercel)
- `dashboard.yourname.com` → Dashboard (Cloud Run)

---

## 🧪 Local Testing

Before deploying, test locally:

```bash
cd web

# Install dependencies (if not already done)
npm install

# Run development server
npm run dev

# Open browser
# Navigate to: http://localhost:3000
# Test the "Launch Dashboard" button
```

---

## 🔄 Updating Your Website

### After Code Changes

```bash
# Commit changes
git add .
git commit -m "Update website"
git push

# Vercel auto-deploys on push (if connected to GitHub)
# OR manually deploy:
vercel --prod
```

### Update Dashboard URL

If your dashboard URL changes:

1. Update environment variable in Vercel dashboard
2. Redeploy
3. OR update `.env.local` for local development

---

## 💰 Cost

- **Vercel Free Tier:**
  - Unlimited deployments
  - 100 GB bandwidth/month
  - Automatic HTTPS
  - Global CDN

**Perfect for resume/portfolio projects!**

---

## 📝 For Your Resume

After deployment:

```
Housing Market Analytics Platform
• Website: https://your-site.vercel.app
• Dashboard: https://housing-dashboard-wo7crn5xwa-uc.a.run.app
• Tech Stack: Next.js (Vercel) + Plotly Dash (Cloud Run) + BigQuery
• Real-time data integration with Reddit, FRED, and Google Trends
```

---

## 🐛 Troubleshooting

### Build Fails

```bash
# Clear cache and rebuild locally
cd web
rm -rf .next node_modules
npm install
npm run build
```

### Dashboard Button Doesn't Work

1. Check environment variable is set correctly
2. Check browser console for errors
3. Verify dashboard URL is accessible

### Changes Not Appearing

- Clear browser cache (Ctrl+Shift+R or Cmd+Shift+R)
- Wait 1-2 minutes for Vercel CDN to propagate
- Check deployment logs in Vercel dashboard

---

## 🎓 Next Steps

1. **Deploy Now:** Run `vercel` from the `web` folder
2. **Add Custom Domain:** (optional) Configure in Vercel settings
3. **Update Resume:** Add both URLs to your resume/portfolio
4. **Share:** Tweet/LinkedIn post about your project!

---

**Ready to deploy?** Run `cd web && vercel` to get started! 🚀
