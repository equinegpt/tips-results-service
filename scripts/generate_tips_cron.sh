#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# generate_tips_cron.sh
#
# Generates daily tips by calling /cron/generate-meeting-tips ONCE PER
# MEETING instead of one massive /cron/generate-daily-tips request.
#
# Calls BOTH iReel and Gemini endpoints per meeting (parallel providers).
#
# This avoids Render's 30-second request timeout killing the job partway
# through. Each per-meeting request takes ~20-30s (10 races × 2s iReel).
# Gemini takes longer (~15-20s per race) but within the 300s curl limit.
#
# Usage (Render cron):
#   bash scripts/generate_tips_cron.sh
#
# Environment:
#   TRS_BASE_URL  - tips-results-service base (default: https://tips-results-service.onrender.com)
#   RA_BASE_URL   - RA crawler base (default: https://ra-crawler.onrender.com)
#   PROJECT_ID    - iReel project ID (required)
#   TARGET_DATE   - override date (default: tomorrow in Melbourne time)
# ---------------------------------------------------------------------------
set -uo pipefail
# NOTE: no -e — we don't want one failed meeting to kill the whole run

TRS="${TRS_BASE_URL:-https://tips-results-service.onrender.com}"
RA="${RA_BASE_URL:-https://ra-crawler.onrender.com}"
PROJECT_ID="${PROJECT_ID:?PROJECT_ID env var is required}"

# Default to tomorrow (Melbourne time) — same as the old cron
if [ -z "${TARGET_DATE:-}" ]; then
  TARGET_DATE=$(TZ="Australia/Melbourne" date -u -d "tomorrow" +%F 2>/dev/null \
    || TZ="Australia/Melbourne" date -v+1d +%F 2>/dev/null \
    || date -u -d "tomorrow" +%F)
fi
export TARGET_DATE

echo "============================================"
echo "[CRON] Tips generation for ${TARGET_DATE}"
echo "[CRON] TRS: ${TRS}"
echo "[CRON] RA:  ${RA}"
echo "============================================"

# ------------------------------------------------------------------
# Step 1: Fetch meetings from RA Crawler and extract unique meetings
#         with meetingId and type (M/P/C)
# ------------------------------------------------------------------
echo "[CRON] Fetching races from RA Crawler..."

RACES_JSON=$(curl -sf "${RA}/races?date=${TARGET_DATE}" 2>/dev/null || echo "[]")

if [ "$RACES_JSON" = "[]" ] || [ -z "$RACES_JSON" ]; then
  echo "[CRON] No races found for ${TARGET_DATE}. Exiting."
  exit 0
fi

# Extract unique meetings: meetingId, track, state, type
# Filter to M (Metro) and P (Provincial) only — same as generate-daily-tips
MEETINGS=$(echo "$RACES_JSON" | python3 -c "
import json, sys, os

target_date = os.environ.get('TARGET_DATE', '')
races = json.load(sys.stdin)

# Filter to target date (RA crawler returns all dates)
if target_date:
    races = [r for r in races if r.get('date') == target_date]

# Group races by meetingId
meetings = {}
for r in races:
    mid = r.get('meetingId')
    if not mid:
        continue
    if mid not in meetings:
        meetings[mid] = {
            'meetingId': mid,
            'track': r.get('track', ''),
            'state': r.get('state', ''),
            'type': (r.get('type') or '').upper(),
            'races': [],
        }
    meetings[mid]['races'].append(r)

# Filter: same rules as daily_generator
# - Always include Metro (M) and Provincial (P)
# - Include Country (C) only if it has a Maiden with prize > 29k
BIG_MAIDEN_THRESHOLD = 29000
filtered = []
for m in meetings.values():
    if m['type'] in ('M', 'P'):
        filtered.append(m)
    elif m['type'] == 'C':
        has_big_maiden = any(
            (r.get('class') or '').lower().startswith('maiden')
            and (r.get('prize') or 0) > BIG_MAIDEN_THRESHOLD
            for r in m['races']
        )
        if has_big_maiden:
            filtered.append(m)

# Sort: Metro first, then Provincial, then Country, then by track name
order = {'M': 0, 'P': 1, 'C': 2}
filtered.sort(key=lambda m: (order.get(m['type'], 9), m['track']))

for m in filtered:
    print(f\"{m['meetingId']}|{m['track']}|{m['state']}|{m['type']}\")
")

if [ -z "$MEETINGS" ]; then
  echo "[CRON] No eligible meetings found for ${TARGET_DATE}. Exiting."
  exit 0
fi

TOTAL=$(echo "$MEETINGS" | wc -l | tr -d ' ')
echo "[CRON] Found ${TOTAL} eligible meetings (M + P + Country with big Maiden)"
echo ""

# ------------------------------------------------------------------
# Step 2: Call iReel + Gemini endpoints per meeting
# ------------------------------------------------------------------
IREEL_SUCCESS=0
IREEL_FAILED=0
IREEL_SKIPPED=0
GEMINI_SUCCESS=0
GEMINI_FAILED=0
GEMINI_SKIPPED=0

while IFS='|' read -r MEETING_ID TRACK STATE TYPE; do
  echo "--------------------------------------------"
  echo "[CRON] ${TRACK} (${STATE}) - ${TYPE} - meetingId=${MEETING_ID}"

  # --- iReel ---
  HTTP_CODE=$(curl -s -o /tmp/trs_response.json -w "%{http_code}" \
    -X POST \
    "${TRS}/cron/generate-meeting-tips?date=${TARGET_DATE}&pf_meeting_id=${MEETING_ID}&project_id=${PROJECT_ID}" \
    --max-time 300)

  if [ "$HTTP_CODE" = "200" ]; then
    RACE_COUNT=$(python3 -c "
import json
try:
    d = json.load(open('/tmp/trs_response.json'))
    print(len(d.get('races', [])))
except:
    print('?')
" 2>/dev/null || echo "?")
    echo "[CRON] ✅ iReel  ${TRACK}: ${RACE_COUNT} races"
    IREEL_SUCCESS=$((IREEL_SUCCESS + 1))

  elif [ "$HTTP_CODE" = "409" ] || [ "$HTTP_CODE" = "422" ]; then
    echo "[CRON] ⏭  iReel  ${TRACK}: skipped (HTTP ${HTTP_CODE})"
    IREEL_SKIPPED=$((IREEL_SKIPPED + 1))

  else
    DETAIL=$(python3 -c "
import json
try:
    d = json.load(open('/tmp/trs_response.json'))
    print(d.get('detail', '(no detail)'))
except:
    print('(could not parse response)')
" 2>/dev/null || echo "(no response)")
    echo "[CRON] ❌ iReel  ${TRACK}: FAILED (HTTP ${HTTP_CODE}) - ${DETAIL}"
    IREEL_FAILED=$((IREEL_FAILED + 1))
  fi

  # Small pause between providers
  sleep 1

  # --- Gemini ---
  HTTP_CODE=$(curl -s -o /tmp/trs_gemini_response.json -w "%{http_code}" \
    -X POST \
    "${TRS}/cron/generate-meeting-tips-gemini?date=${TARGET_DATE}&pf_meeting_id=${MEETING_ID}" \
    --max-time 600)

  if [ "$HTTP_CODE" = "200" ]; then
    RACE_COUNT=$(python3 -c "
import json
try:
    d = json.load(open('/tmp/trs_gemini_response.json'))
    print(len(d.get('races', [])))
except:
    print('?')
" 2>/dev/null || echo "?")
    echo "[CRON] ✅ Gemini ${TRACK}: ${RACE_COUNT} races"
    GEMINI_SUCCESS=$((GEMINI_SUCCESS + 1))

  elif [ "$HTTP_CODE" = "409" ] || [ "$HTTP_CODE" = "422" ]; then
    echo "[CRON] ⏭  Gemini ${TRACK}: skipped (HTTP ${HTTP_CODE})"
    GEMINI_SKIPPED=$((GEMINI_SKIPPED + 1))

  else
    DETAIL=$(python3 -c "
import json
try:
    d = json.load(open('/tmp/trs_gemini_response.json'))
    print(d.get('detail', '(no detail)'))
except:
    print('(could not parse response)')
" 2>/dev/null || echo "(no response)")
    echo "[CRON] ❌ Gemini ${TRACK}: FAILED (HTTP ${HTTP_CODE}) - ${DETAIL}"
    GEMINI_FAILED=$((GEMINI_FAILED + 1))
  fi

  # Pause between meetings
  sleep 2

done <<< "$MEETINGS"

# ------------------------------------------------------------------
# Step 3: Cache Sportsbet event IDs for today's races
# (Fetches each tipped track page via Scrape.do while races are live)
# ------------------------------------------------------------------
echo ""
echo "[CRON] Caching SB event IDs for ${TARGET_DATE}..."
SB_CACHE_CODE=$(curl -s -o /tmp/sb_cache.json -w "%{http_code}" \
  -X POST "${RA}/cache-sb-events?date=${TARGET_DATE}" \
  --max-time 300)

if [ "$SB_CACHE_CODE" = "200" ]; then
  CACHED=$(python3 -c "import json; print(json.load(open('/tmp/sb_cache.json')).get('cached', 0))" 2>/dev/null || echo "?")
  echo "[CRON] ✅ Cached ${CACHED} SB event IDs for exotic scraping tonight"
else
  echo "[CRON] ⚠️  SB cache failed (HTTP ${SB_CACHE_CODE})"
fi

echo ""
echo "============================================"
echo "[CRON] DONE for ${TARGET_DATE}"
echo "[CRON] iReel:  ✅ ${IREEL_SUCCESS}  ⏭ ${IREEL_SKIPPED}  ❌ ${IREEL_FAILED}"
echo "[CRON] Gemini: ✅ ${GEMINI_SUCCESS}  ⏭ ${GEMINI_SKIPPED}  ❌ ${GEMINI_FAILED}"
echo "============================================"

# Exit with error if ALL meetings failed for BOTH providers
if [ "$IREEL_SUCCESS" -eq 0 ] && [ "$IREEL_FAILED" -gt 0 ] \
   && [ "$GEMINI_SUCCESS" -eq 0 ] && [ "$GEMINI_FAILED" -gt 0 ]; then
  echo "[CRON] All meetings failed for both providers!"
  exit 1
fi

exit 0
