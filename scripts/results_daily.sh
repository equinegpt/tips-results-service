#!/usr/bin/env bash
set -euo pipefail

echo "[results_daily.sh] Start $(date)"
echo "[results_daily.sh] DATABASE_URL=\${DATABASE_URL:-unset}"
echo "[results_daily.sh] RA_CRAWLER_BASE_URL=\${RA_CRAWLER_BASE_URL:-https://ra-crawler.onrender.com}"

python -m app.results_daily_job

echo "[results_daily.sh] Done  $(date)"
