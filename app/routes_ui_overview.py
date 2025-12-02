# app/routes_ui_overview.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any, List, Tuple

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo

from .database import get_db
from .models import Meeting, Race, Tip, TipOutcome
from .ra_results_client import RAResultsClient
from .daily_generator import _tracks_match  # reuse the same fuzzy track matcher

router = APIRouter()


def _today_melb() -> date:
    return datetime.now(ZoneInfo("Australia/Melbourne")).date()


def _parse_date_param(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


def _classify_outcome_from_pos(pos_fin: Optional[int]) -> str:
    """
    Map finishing position from RA results to a TipOutcome-style status.
    """
    if pos_fin is None or pos_fin <= 0:
        return "PENDING"
    if pos_fin == 1:
        return "WIN"
    if pos_fin in (2, 3):
        return "PLACE"
    return "LOSE"


def _build_ra_results_index(
    d_from: date,
    d_to: date,
) -> Dict[Tuple[date, str, int, int], List[Any]]:
    """
    Build an index over RA Crawler results for the date window [d_from, d_to].

    Key:   (meeting_date, STATE, race_no, tab_number)
    Value: list[RAResultRow]  (we use _tracks_match on track name when needed)
    """
    index: Dict[Tuple[date, str, int, int], List[Any]] = {}

    client = RAResultsClient()
    try:
        day = d_from
        while day <= d_to:
            try:
                rows = client.fetch_results_for_date(day)
                print(f"[OVR] RA rows for {day}: {len(rows)}")
            except Exception as e:
                print(f"[OVR] error fetching RA rows for {day}: {e}")
                rows = []

            for r in rows:
                key = (
                    r.meeting_date,
                    (r.state or "").upper(),
                    r.race_no,
                    r.tab_number,
                )
                index.setdefault(key, []).append(r)

            day += timedelta(days=1)
    finally:
        client.close()

    total = sum(len(v) for v in index.values())
    print(f"[OVR] RA index total rows = {total}")
    return index


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

    Source-of-truth for results:
      • FIRST: RA Crawler results (via RAResultsClient)
      • FALLBACK: TipOutcome rows (legacy PF import)

    That way this matches /ui/day, which is now also driven off RA.
    """
    today = _today_melb()

    d_to = _parse_date_param(date_to) or today
    d_from = _parse_date_param(date_from) or (d_to - timedelta(days=6))

    # 1) Build RA results index for the window
    ra_index = _build_ra_results_index(d_from, d_to)

    # 2) Fetch all tips + (optional) outcomes in that window
    rows = (
        db.query(Tip, TipOutcome, Race, Meeting)
        .join(Race, Tip.race_id == Race.id)
        .join(Meeting, Race.meeting_id == Meeting.id)
        .outerjoin(TipOutcome, TipOutcome.tip_id == Tip.id)
        .filter(Meeting.date >= d_from, Meeting.date <= d_to)
        .all()
    )
    print(f"[OVR] raw rows (Tip+Outcome+Race+Meeting) in window {d_from} → {d_to}: {len(rows)}")

    # 3) Aggregate per (track, state)
    agg: Dict[tuple[str, str], Dict[str, Any]] = {}

    for tip, outcome, race, meeting in rows:
        key_track = meeting.track_name
        key_state = meeting.state

        key = (key_track, key_state)
        if key not in agg:
            agg[key] = {
                "track": key_track,
                "state": key_state,
                "tips": 0,
                "wins": 0,
                "places": 0,
                "stakes": Decimal("0.0"),
                "return": Decimal("0.0"),
                # per-track date range within the window
                "min_date": meeting.date,
                "max_date": meeting.date,
            }

        bucket = agg[key]
        bucket["tips"] += 1

        # track min/max date for this track in the window
        if meeting.date < bucket["min_date"]:
            bucket["min_date"] = meeting.date
        if meeting.date > bucket["max_date"]:
            bucket["max_date"] = meeting.date

        # Stake units (Numeric in DB) → Decimal
        stake_units = Decimal(str(tip.stake_units or 1))
        bucket["stakes"] += stake_units

        # -----------------------------
        # RA-FIRST status + SP
        # -----------------------------
        status: Optional[str] = None
        sp_src: Any = None

        # Base key ignoring track name (we'll use _tracks_match for it)
        ra_key = (
            meeting.date,
            (meeting.state or "").upper(),
            race.race_number,
            tip.tab_number,
        )
        candidates = ra_index.get(ra_key) or []
        ra_row = None

        if candidates:
            mt_track = meeting.track_name or ""
            if len(candidates) == 1:
                ra_row = candidates[0]
            else:
                # Use same fuzzy track matcher as daily generator
                for cand in candidates:
                    if _tracks_match(mt_track, cand.track):
                        ra_row = cand
                        break
                if ra_row is None:
                    ra_row = candidates[0]

        if ra_row is not None:
            # RA is canonical: scratched runners are "SCRATCHED",
            # otherwise classify by finishing_pos.
            if getattr(ra_row, "is_scratched", False):
                status = "SCRATCHED"
            else:
                status = _classify_outcome_from_pos(ra_row.finishing_pos)
            sp_src = ra_row.starting_price

            print(
                f"[OVR] RA primary {meeting.track_name} {meeting.state} "
                f"{meeting.date} R{race.race_number} #{tip.tab_number}: "
                f"pos={ra_row.finishing_pos}, sp={ra_row.starting_price}, "
                f"status={status}"
            )
        else:
            # No RA row → fall back to TipOutcome (legacy PF results)
            if outcome is not None:
                status = outcome.outcome_status or "PENDING"
                sp_src = outcome.starting_price
            else:
                status = "PENDING"
                sp_src = None

        # -----------------------------
        # Status → wins / places / return
        # -----------------------------
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
        if isinstance(sp_src, (Decimal, float, int)):
            sp_dec = Decimal(str(sp_src))
        else:
            sp_dec = None

        if status == "WIN" and sp_dec is not None:
            bucket["return"] += stake_units * sp_dec

    # 4) Convert aggregates to list + compute strike rates & ROI
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
                "firstDate": b["min_date"].isoformat(),
                "lastDate": b["max_date"].isoformat(),
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

    # 5) Render HTML
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
              <td>{t["firstDate"]} → {t["lastDate"]}</td>
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
      .controls form {{
        display: inline-block;
        margin-right: 16px;
      }}
      .controls label {{
        margin-right: 8px;
        font-size: 13px;
        color: #ccc;
      }}
      .controls input[type="date"] {{
        background: #11131a;
        border: 1px solid #333;
        color: #f5f5f5;
        border-radius: 4px;
        padding: 2px 4px;
      }}
      .controls button {{
        background: #18CB96;
        border: none;
        color: #0b0c10;
        border-radius: 4px;
        padding: 4px 10px;
        font-size: 13px;
        cursor: pointer;
      }}
      .controls a {{
        color: #18CB96;
        text-decoration: none;
        margin-right: 12px;
        font-size: 13px;
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
      <form method="get" action="/ui/overview">
        <label>
          From
          <input type="date" name="date_from" value="{payload["dateFrom"]}">
        </label>
        <label>
          To
          <input type="date" name="date_to" value="{payload["dateTo"]}">
        </label>
        <button type="submit">Apply</button>
      </form>
      <a href="/ui/overview?date_from={payload["dateFrom"]}&date_to={payload["dateTo"]}&json=1">
        View as JSON
      </a>
    </div>
    <table>
      <thead>
        <tr>
          <th>Dates</th>
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
        {''.join(html_rows) if html_rows else '<tr><td colspan="11">No tips in this window.</td></tr>'}
      </tbody>
    </table>
  </body>
</html>
"""
    return HTMLResponse(content=html)
