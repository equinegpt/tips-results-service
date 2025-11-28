# app/routes_ui_overview.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo

from .database import get_db
from .models import Meeting, Race, Tip, TipOutcome

router = APIRouter()


def _today_melb() -> date:
    return datetime.now(ZoneInfo("Australia/Melbourne")).date()


def _parse_date_param(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


@router.get("/ui/overview", response_class=HTMLResponse)
def ui_overview(
    date_from: Optional[str] = Query(
        None,
        description="Start date (YYYY-MM-DD). Default: 7 days ago (incl. today).",
    ),
    date_to: Optional[str] = Query(
        None,
        description="End date (YYYY-MM-DD). Default: today (Melbourne).",
    ),
    json: bool = Query(
        False,
        description="If true, return raw JSON instead of HTML.",
    ),
    db: Session = Depends(get_db),
):
    """
    Overview of tips & results across tracks for a date window.

    - Default: renders an HTML table (for browser use).
    - If `?json=1` is passed, returns the raw JSON payload instead.
    """
    today = _today_melb()

    d_to = _parse_date_param(date_to) or today
    d_from = _parse_date_param(date_from) or (d_to - timedelta(days=6))

    # Fetch all tips + outcomes in that window
    rows = (
        db.query(Tip, TipOutcome, Race, Meeting)
        .join(Race, Tip.race_id == Race.id)
        .join(Meeting, Race.meeting_id == Meeting.id)
        .outerjoin(TipOutcome, TipOutcome.tip_id == Tip.id)
        .filter(Meeting.date >= d_from, Meeting.date <= d_to)
        .all()
    )

    # Aggregate per (track, state)
    agg: Dict[tuple[str, str], Dict[str, Any]] = {}

    for tip, outcome, race, meeting in rows:
        key = (meeting.track_name, meeting.state)
        if key not in agg:
            agg[key] = {
                "track": meeting.track_name,
                "state": meeting.state,
                "tips": 0,
                "wins": 0,
                "places": 0,
                "stakes": Decimal("0.0"),
                "return": Decimal("0.0"),
            }

        bucket = agg[key]
        bucket["tips"] += 1

        # Stake units (Numeric in DB) → Decimal
        stake_units = Decimal(str(tip.stake_units or 1))
        bucket["stakes"] += stake_units

        status = outcome.outcome_status if outcome is not None else "PENDING"

        if status == "WIN":
            bucket["wins"] += 1
            bucket["places"] += 1
        elif status == "PLACE":
            bucket["places"] += 1
        elif status in ("SCRATCHED", "NO_RESULT", "PENDING"):
            # zero P&L, but still counted as a tip
            pass
        else:
            # LOSE or anything else → zero P&L
            pass

        # Simple return calc: for winners, SP * stake; others 0
        sp = outcome.starting_price if outcome is not None else None
        if isinstance(sp, (Decimal, float, int)):
            sp_dec = Decimal(str(sp))
        else:
            sp_dec = None

        if status == "WIN" and sp_dec is not None:
            bucket["return"] += stake_units * sp_dec

    # Convert aggregates to list + compute strike rates & ROI
    tracks: list[Dict[str, Any]] = []
    for (track_name, state), b in agg.items():
        tips = b["tips"]
        wins = b["wins"]
        places = b["places"]
        stakes = b["stakes"]
        ret = b["return"]

        win_sr = float(wins) / tips if tips else 0.0
        place_sr = float(places) / tips if tips else 0.0
        roi = float(ret / stakes) - 1.0 if stakes > 0 else 0.0

        tracks.append(
            {
                "track": track_name,
                "state": state,
                "tips": tips,
                "wins": wins,
                "places": places,
                "winStrikeRate": win_sr,
                "placeStrikeRate": place_sr,
                "stakes": float(stakes),
                "return": float(ret),
                "roi": roi,
            }
        )

    # Sort: best ROI first, then by tips desc
    tracks.sort(key=lambda t: (t["roi"], t["tips"]), reverse=True)

    payload = {
        "dateFrom": d_from.isoformat(),
        "dateTo": d_to.isoformat(),
        "tracks": tracks,
    }

    # If caller wants JSON, short-circuit here
    if json:
        return JSONResponse(payload)

    # Otherwise render simple HTML
    html_rows = []
    for t in tracks:
        win_sr_pct = 100.0 * t["winStrikeRate"]
        place_sr_pct = 100.0 * t["placeStrikeRate"]
        roi_pct = 100.0 * t["roi"]

        # Colour-code ROI
        if t["roi"] > 0:
            roi_class = "roi-pos"
        elif t["roi"] < 0:
            roi_class = "roi-neg"
        else:
            roi_class = "roi-zero"

        html_rows.append(
            f"""
            <tr>
              <td>{t["track"]}</td>
              <td>{t["state"]}</td>
              <td style="text-align:right">{t["tips"]}</td>
              <td style="text-align:right">{t["wins"]}</td>
              <td style="text-align:right">{t["places"]}</td>
              <td style="text-align:right">{win_sr_pct:.1f}%</td>
              <td style="text-align:right">{place_sr_pct:.1f}%</td>
              <td style="text-align:right">{t["stakes"]:.1f}</td>
              <td style="text-align:right">{t["return"]:.2f}</td>
              <td class="{roi_class}" style="text-align:right">{roi_pct:.1f}%</td>
            </tr>
            """
        )

    html = f"""
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Tips Overview ({payload["dateFrom"]} → {payload["dateTo"]})</title>
    <style>
      body {{
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        padding: 16px;
        background-color: #0b0c10;
        color: #f5f5f5;
      }}
      h1 {{
        margin-bottom: 4px;
      }}
      .meta {{
        color: #aaa;
        margin-bottom: 16px;
      }}
      table {{
        border-collapse: collapse;
        width: 100%;
        font-size: 14px;
      }}
      th, td {{
        padding: 6px 8px;
        border-bottom: 1px solid #333;
      }}
      th {{
        text-align: left;
        background-color: #151820;
        position: sticky;
        top: 0;
        z-index: 1;
      }}
      tr:nth-child(even) {{
        background-color: #151820;
      }}
      tr:nth-child(odd) {{
        background-color: #11131a;
      }}
      .roi-pos {{
        color: #18CB96;
        font-weight: 600;
      }}
      .roi-neg {{
        color: #ff6b6b;
        font-weight: 600;
      }}
      .roi-zero {{
        color: #ddd;
      }}
      .pill {{
        display: inline-block;
        padding: 2px 8px;
        border-radius: 999px;
        background: #151820;
        font-size: 12px;
        margin-left: 4px;
      }}
      .controls {{
        margin-bottom: 12px;
      }}
      .controls a {{
        color: #18CB96;
        text-decoration: none;
        margin-right: 12px;
      }}
    </style>
  </head>
  <body>
    <h1>Tips Overview</h1>
    <div class="meta">
      Window: <strong>{payload["dateFrom"]}</strong> → <strong>{payload["dateTo"]}</strong>
      <span class="pill">tracks: {len(tracks)}</span>
      <span class="pill">tips: {sum(t["tips"] for t in tracks)}</span>
    </div>
    <div class="controls">
      <a href="/ui/overview?date_from={payload["dateFrom"]}&date_to={payload["dateTo"]}&json=1">
        View as JSON
      </a>
    </div>
    <table>
      <thead>
        <tr>
          <th>Track</th>
          <th>State</th>
          <th style="text-align:right">Tips</th>
          <th style="text-align:right">Wins</th>
          <th style="text-align:right">Places</th>
          <th style="text-align:right">Win SR</th>
          <th style="text-align:right">Place SR</th>
          <th style="text-align:right">Stakes</th>
          <th style="text-align:right">Return</th>
          <th style="text-align:right">ROI</th>
        </tr>
      </thead>
      <tbody>
        {''.join(html_rows) if html_rows else '<tr><td colspan="10">No tips in this window.</td></tr>'}
      </tbody>
    </table>
  </body>
</html>
"""
    return HTMLResponse(content=html)
