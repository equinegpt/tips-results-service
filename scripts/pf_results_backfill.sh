#!/usr/bin/env bash
set -euo pipefail

# Backfill PF results for today-2, today-3, today-4 (UTC)
for OFFSET in 2 3 4; do
  TARGET_DATE=$(date -u -d "${OFFSET} days ago" +%F)
  echo "[CRON] Fetching PF results for ${TARGET_DATE}"
  curl -X POST "https://tips-results-service.onrender.com/cron/fetch-pf-results?date=${TARGET_DATE}"
done
