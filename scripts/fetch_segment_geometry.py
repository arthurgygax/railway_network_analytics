"""Fetch real track geometry for each map segment and upload it for the dashboard.

Straight lines between stations are wrong in a way that matters: Köln -> Frankfurt
Flughafen drawn straight is 152 km across the Westerwald; the actual Köln-Rhein/Main
high-speed line is 168 km via Montabaur.

Geometry comes from OpenRailRouting (https://routing.openrailrouting.org), a GraphHopper
fork that routes on OpenStreetMap railway tracks. Data (c) OpenStreetMap contributors,
ODbL — the dashboard must carry that attribution.

Results are cached on disk, so re-runs cost nothing and we stay polite to a free
public service.

    set -a; source .env; set +a; uv run python scripts/fetch_segment_geometry.py
"""

from __future__ import annotations

import io
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from databricks.sdk import WorkspaceClient

ROUTER = "https://routing.openrailrouting.org/route"
PROFILE = "all_tracks_1435"      # standard gauge; tgv_all restricts to high-speed
CACHE = Path("data/raw/geometry/segments.json")
VOLUME = os.environ.get("DATABRICKS_REFERENCE_VOLUME", "/Volumes/railway/raw/reference")
WAREHOUSE = os.environ.get("DATABRICKS_WAREHOUSE_ID", "34c382fa6e1657ad")
SLEEP = 0.3                      # free public service; do not hammer it


def segments(w: WorkspaceClient) -> list[dict]:
    """Adjacent segments only. hops > 1 spans stations we never polled, so its 'track'
    would be a guess about a path we have no evidence for."""
    from databricks.sdk.service.sql import StatementState

    sql = """
        SELECT DISTINCT from_station, to_station,
               from_latitude, from_longitude, to_latitude, to_longitude
        FROM railway.gold.segment_performance
        WHERE hops = 1
          AND from_latitude IS NOT NULL AND to_latitude IS NOT NULL
    """
    r = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=WAREHOUSE, wait_timeout="50s"
    )
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(r.status.error.message if r.status.error else r.status.state)
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row, strict=True)) for row in (r.result.data_array or [])]


def route(from_lat, from_lon, to_lat, to_lon) -> tuple[list, float] | None:
    query = urllib.parse.urlencode([
        ("point", f"{from_lat},{from_lon}"),
        ("point", f"{to_lat},{to_lon}"),
        ("profile", PROFILE),
        ("points_encoded", "false"),
    ])
    try:
        with urllib.request.urlopen(f"{ROUTER}?{query}", timeout=60) as response:
            payload = json.loads(response.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None
    finally:
        time.sleep(SLEEP)
    paths = payload.get("paths")
    if not paths:
        return None
    return paths[0]["points"]["coordinates"], paths[0]["distance"] / 1000


def main() -> None:
    w = WorkspaceClient()
    pairs = segments(w)
    print(f"{len(pairs)} adjacent segments to resolve")

    cache = json.loads(CACHE.read_text(encoding="utf-8")) if CACHE.is_file() else {}
    resolved = failed = cached = 0

    for pair in pairs:
        key = f"{pair['from_station']}|{pair['to_station']}"
        if key in cache:
            cached += 1
            continue
        result = route(pair["from_latitude"], pair["from_longitude"],
                       pair["to_latitude"], pair["to_longitude"])
        if result is None:
            failed += 1
            print(f"  no route: {key}")
            continue
        coordinates, km = result
        cache[key] = {
            "from_station": pair["from_station"],
            "to_station": pair["to_station"],
            "track_km": round(km, 1),
            "coordinates": coordinates,
        }
        resolved += 1

    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(cache), encoding="utf-8")
    print(f"  resolved {resolved}, cached {cached}, failed {failed}")

    body = "".join(json.dumps(v, ensure_ascii=False) + "\n" for v in cache.values())
    w.files.upload(f"{VOLUME}/segment_geometry.jsonl",
                   io.BytesIO(body.encode("utf-8")), overwrite=True)
    print(f"{len(cache)} geometries -> {VOLUME}/segment_geometry.jsonl "
          f"({len(body):,} bytes)")


if __name__ == "__main__":
    main()
