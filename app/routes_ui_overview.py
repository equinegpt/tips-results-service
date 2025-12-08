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

    Aggregation is per (meeting date, track, state) so each line
    is a single meeting/day, not merged across the window.
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
    print(
        f"[OVR] raw rows (Tip+Outcome+Race+Meeting) in window "
        f"{d_from} → {d_to}: {len(rows)}"
    )

    # 3) Aggregate per (date, track, state)
    agg: Dict[tuple[date, str, str], Dict[str, Any]] = {}

    for tip, outcome, race, meeting in rows:
        key_date = meeting.date
        key_track = meeting.track_name
        key_state = meeting.state

        key = (key_date, key_track, key_state)
        if key not in agg:
            agg[key] = {
                "date": key_date,
                "track": key_track,
                "state": key_state,
                "tips": 0,
                "wins": 0,
                "places": 0,
                "stakes": Decimal("0.0"),
                "return": Decimal("0.0"),
                # NEW: tracking AI-best wins and per-race positions
                "ai_best_wins": 0,
                "race_positions": {},  # race.id -> set of finishing positions
            }

        bucket = agg[key]
        bucket["tips"] += 1

        # Stake units (Numeric in DB) → Decimal
        stake_units = Decimal(str(tip.stake_units or 1))
        bucket["stakes"] += stake_units

        # -----------------------------
        # Detect whether this Tip is "AI Best"
        # -----------------------------
        ai_is_best = False

        # Common pattern: numeric slot/rank == 1
        slot = getattr(tip, "slot", None)
        if isinstance(slot, int) and slot == 1:
            ai_is_best = True
        elif isinstance(slot, str) and slot.strip().lower() in {
            "best",
            "ai_best",
            "ai best",
            "ai-best",
        }:
            ai_is_best = True

        # Fallback: use a string type/kind/label field if present
        label = (
            getattr(tip, "tip_type", None)
            or getattr(tip, "kind", None)
            or getattr(tip, "role", None)
            or getattr(tip, "tip_label", None)
        )
        if isinstance(label, str) and label.strip().lower() in {
            "best",
            "ai_best",
            "ai best",
            "ai-best",
        }:
            ai_is_best = True

        # -----------------------------
        # RA-FIRST status + SP + finishing pos
        # -----------------------------
        status: Optional[str] = None
        sp_src: Any = None
        pos: Optional[int] = None  # finishing position if known

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
            pos = getattr(ra_row, "finishing_pos", None)
            # RA is canonical: scratched runners are "SCRATCHED",
            # otherwise classify by finishing_pos.
            if getattr(ra_row, "is_scratched", False):
                status = "SCRATCHED"
            else:
                status = _classify_outcome_from_pos(pos)
            sp_src = ra_row.starting_price

            print(
                f"[OVR] RA primary {meeting.track_name} {meeting.state} "
                f"{meeting.date} R{race.race_number} #{tip.tab_number}: "
                f"pos={pos}, sp={ra_row.starting_price}, "
                f"status={status}"
            )
        else:
            # No RA row → fall back to TipOutcome (legacy PF results)
            if outcome is not None:
                status = outcome.outcome_status or "PENDING"
                # Best-effort grab of a finishing position from outcome
                pos = (
                    getattr(outcome, "finishing_pos", None)
                    or getattr(outcome, "position", None)
                    or getattr(outcome, "outcome_position", None)
                )
                sp_src = outcome.starting_price
            else:
                status = "PENDING"
                sp_src = None
                pos = None

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

        # Count AI Best wins
        if status == "WIN" and ai_is_best:
            bucket["ai_best_wins"] += 1

        # Track race-wise finishing positions (for Quinella / Trifecta)
        if isinstance(pos, int) and pos in (1, 2, 3):
            race_pos_map = bucket["race_positions"]
            race_set = race_pos_map.setdefault(race.id, set())
            race_set.add(int(pos))

        # Simple return calc: for winners, SP * stake; others 0
        if isinstance(sp_src, (Decimal, float, int)):
            sp_dec = Decimal(str(sp_src))
        else:
            sp_dec = None

        if status == "WIN" and sp_dec is not None:
            bucket["return"] += stake_units * sp_dec

    # 4) Convert aggregates to list + compute strike rates, ROI, Quin, Tri
    tracks: list[Dict[str, Any]] = []
    for (row_date, track_name, state), b in agg.items():
        tips = b["tips"]
        wins = b["wins"]
        places = b["places"]
        stakes = b["stakes"]
        ret = b["return"]
        ai_best_wins = b.get("ai_best_wins", 0)

        win_sr = float(wins) / tips if tips else 0.0
        place_sr = float(places) / tips if tips else 0.0
        roi = float(ret / stakes) - 1.0 if stakes > 0 else 0.0

        # Compute Quinella / Trifecta counts from race_positions
        race_positions = b.get("race_positions", {})
        quin = 0
        tri = 0
        for positions in race_positions.values():
            if 1 in positions and 2 in positions:
                quin += 1
            if 1 in positions and 2 in positions and 3 in positions:
                tri += 1

        tracks.append(
            {
                "date": row_date.isoformat(),
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
                # NEW fields in JSON payload
                "aiBestWins": ai_best_wins,
                "quinellas": quin,
                "trifectas": tri,
            }
        )

    # Sort: latest date first, then best ROI, then tips desc
    tracks.sort(key=lambda t: (t["date"], t["roi"], t["tips"]), reverse=True)

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
              <td>{t["date"]}</td>
              <td>{t["track"]}</td>
              <td>{t["state"]}</td>
              <td style="text-align:right">{t["tips"]}</td>
              <td style="text-align:right">{t["wins"]}</td>
              <td style="text-align:right">{t.get("aiBestWins", 0)}</td>
              <td style="text-align:right">{t.get("quinellas", 0)}</td>
              <td style="text-align:right">{t.get("trifectas", 0)}</td>
              <td style="text-align:right">{win_sr_pct:.1f}%</td>
              <td style="text-align:right">{place_sr_pct:.1f}%</td>
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
      <span class="pill">rows: {len(tracks)}</span>
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
          <th>Date</th>
          <th>Track</th>
          <th>State</th>
          <th style="text-align:right">Tips</th>
          <th style="text-align:right">Wins</th>
          <th style="text-align:right">AI Best Wins</th>
          <th style="text-align:right">Quinellas</th>
          <th style="text-align:right">Trifectas</th>
          <th style="text-align:right">Win SR</th>
          <th style="text-align:right">Place SR</th>
          <th style="text-align:right">Return</th>
          <th style="text-align:right">ROI</th>
        </tr>
      </thead>
      <tbody>
        {''.join(html_rows) if html_rows else '<tr><td colspan="12">No tips in this window.</td></tr>'}
      </tbody>
    </table>
  </body>
</html>
"""
    return HTMLResponse(content=html)
