# scripts/publish_tips_to_stablfy.py

import os
import sys
import datetime as dt
from zoneinfo import ZoneInfo

import requests

# Base URL of your tips service
TIPS_BASE_URL = os.getenv(
    "TIPS_BASE_URL",
    "https://tips-results-service.onrender.com",
)

# Webhook URL for Uncanny Automator on stablfy.com
STABLFY_WEBHOOK_URL = os.environ["STABLFY_WEBHOOK_URL"]  # set this in Render

MEL_TZ = ZoneInfo("Australia/Melbourne")


def resolve_date_arg() -> str:
    """
    If you pass a date on the command line, use that.
    Otherwise use 'today' in Australia/Melbourne.
    """
    if len(sys.argv) > 1:
        return sys.argv[1]

    now_mel = dt.datetime.now(MEL_TZ)
    return now_mel.strftime("%Y-%m-%d")


def main():
    date_str = resolve_date_arg()

    # 1) Pull compiled tips from your service
    resp = requests.get(
        f"{TIPS_BASE_URL}/tips",
        params={"date": date_str},
        timeout=30,
    )
    resp.raise_for_status()
    tips_payload = resp.json()

    payload = {
        "date": date_str,
        "source": "tips-results-service",
        "tips": tips_payload,
    }

    out = requests.post(
        STABLFY_WEBHOOK_URL,
        json=payload,
        timeout=30,
    )

    # Debug-friendly error handling
    try:
        out.raise_for_status()
    except requests.HTTPError:
        print("❌ Error from WordPress / Uncanny Automator")
        print(f"Status: {out.status_code}")
        print("Response body:")
        print(out.text)
        raise

    print(
        f"Published tips for {date_str} → {STABLFY_WEBHOOK_URL} "
        f"(status={out.status_code})"
    )

if __name__ == "__main__":
    main()
