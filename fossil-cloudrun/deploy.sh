#!/usr/bin/env bash
# ── Fossil Dashboard — Cloud Run Deploy Script ───────────────────────────
# Usage: ./deploy.sh
# Prerequisites:
#   - gcloud CLI installed and authenticated (gcloud auth login)
#   - gcloud config set project YOUR_PROJECT_ID
#   - Artifact Registry API enabled
#   - Cloud Run API enabled
# ─────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Config — edit these ──────────────────────────────────────────────────
PROJECT_ID="${GCP_PROJECT:-$(gcloud config get-value project)}"
REGION="us-east1"
SERVICE_NAME="fossil-dashboard"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
CUSTOM_DOMAIN="dmv-fossils.com"
# ─────────────────────────────────────────────────────────────────────────

echo "🦕 Building and deploying Fossil Dashboard to Cloud Run"
echo "   Project : ${PROJECT_ID}"
echo "   Region  : ${REGION}"
echo "   Image   : ${IMAGE}"
echo ""

# Copy app files into build context (they live one level up)
cp ../fossil_server.py .
cp ../fossil_hunting_sites.json .
cp ../index.html .

# Build and push image
echo "📦 Building image..."
gcloud builds submit --tag "${IMAGE}" .

# Deploy to Cloud Run
echo "🚀 Deploying to Cloud Run..."
gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE}" \
  --platform managed \
  --region "${REGION}" \
  --allow-unauthenticated \
  --port 8080 \
  --memory 512Mi \
  --cpu 1 \
  --min-instances 1 \
  --max-instances 3 \
  --cpu-boost \
  --set-env-vars "APPLICATION_ROOT=/fossil" \
  --ingress all

# Print service URL
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --platform managed \
  --region "${REGION}" \
  --format "value(status.url)")
echo ""
echo "✅ Deployed! Service URL: ${SERVICE_URL}"
echo "   Dashboard: ${SERVICE_URL}/fossil"

# Optional: custom domain mapping
if [[ -n "${CUSTOM_DOMAIN}" ]]; then
  echo ""
  echo "🌐 Mapping custom domain: ${CUSTOM_DOMAIN}"
  gcloud beta run domain-mappings create \
    --service "${SERVICE_NAME}" \
    --domain "${CUSTOM_DOMAIN}" \
    --region "${REGION}" || true

  echo ""
  echo "⚠️  Add these DNS records at your registrar:"
  gcloud beta run domain-mappings describe \
    --domain "${CUSTOM_DOMAIN}" \
    --region "${REGION}" \
    --format "table(status.resourceRecords[].name, status.resourceRecords[].type, status.resourceRecords[].rrdata)"
fi

echo ""
echo "🔒 HTTPS is handled automatically by Google Cloud Run."
echo "   Let's Encrypt cert will be provisioned within ~15 minutes of DNS propagation."
