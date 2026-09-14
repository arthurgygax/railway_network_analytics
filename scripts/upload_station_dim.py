"""Build the station dimension and upload it for the dashboard map.

The map needs coordinates, which live in the cached StaDa data but never made it into
poll_targets.json. StaDa covers German stations only, so the two Swiss ones are added
by hand below.

Uploads to a SEPARATE volume, not `landing`: Bronze streams every file under landing
with a fixed Kafka-record schema, so a dimension file there would land in
_corrupt_record.

    set -a; source .env; set +a; uv run python scripts/upload_station_dim.py
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient

TARGETS = Path("data/raw/stada/poll_targets.json")
STADA = Path("data/raw/stada/stations.json")
VOLUME = os.environ.get("DATABRICKS_REFERENCE_VOLUME", "/Volumes/railway/raw/reference")

# StaDa is German stations only. These two are on our polled list but Swiss, so their
# coordinates come from OpenStreetMap rather than the API.
FOREIGN = {
    8500010: ("Basel SBB", 47.5474, 7.5898),
    8000026: ("Basel Bad Bf", 47.5672, 7.6077),
}


def build() -> list[dict]:
    targets = json.loads(TARGETS.read_text(encoding="utf-8"))
    stada = json.loads(STADA.read_text(encoding="utf-8"))["result"]
    by_eva = {e["number"]: (s, e) for s in stada for e in s.get("evaNumbers", [])}

    rows = []
    for target in targets:
        eva = target["eva"]
        hit = by_eva.get(eva)
        coords = (hit[1].get("geographicCoordinates") or {}).get("coordinates") if hit else None
        if coords:
            lon, lat, source = coords[0], coords[1], "stada"
        elif eva in FOREIGN:
            _, lat, lon, source = *FOREIGN[eva], "manual"
        else:
            lat = lon = None
            source = "missing"
        rows.append({
            "station_eva": eva,
            "station_name": target["name"],          # the name Timetables uses — the join key
            "stada_name": target.get("stada_name"),
            "latitude": lat,
            "longitude": lon,
            "coordinate_source": source,
            "category": target.get("category"),
            "federal_state": target.get("federal_state"),
            "route_calls": target.get("route_calls"),
        })
    return rows


def main() -> None:
    rows = build()
    missing = [r["station_name"] for r in rows if r["latitude"] is None]
    body = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode("utf-8")

    w = WorkspaceClient()
    w.files.upload(f"{VOLUME}/stations.jsonl", io.BytesIO(body), overwrite=True)

    print(f"{len(rows)} stations -> {VOLUME}/stations.jsonl ({len(body):,} bytes)")
    print(f"  with coordinates: {sum(1 for r in rows if r['latitude'] is not None)}")
    if missing:
        print(f"  STILL MISSING: {missing}")


if __name__ == "__main__":
    main()
