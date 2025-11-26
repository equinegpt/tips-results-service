# app/services/tracks.py
from typing import List, Dict, Any
from sqlalchemy.orm import Session

from app.models import Meeting  # adjust import to your layout


def get_all_tracks(db: Session) -> List[Dict[str, Any]]:
    """
    Return list of dicts:
    [
      { "code": "vic|flemington", "track_name": "Flemington", "state": "VIC" },
      ...
    ]
    """
    rows = (
        db.query(Meeting.state, Meeting.track_name)
        .distinct()
        .order_by(Meeting.state, Meeting.track_name)
        .all()
    )

    result: List[Dict[str, Any]] = []
    for state, track_name in rows:
        code = f"{state.lower()}|{track_name.lower()}"
        result.append(
            {
                "code": code,
                "track_name": track_name,
                "state": state,
            }
        )
    return result
