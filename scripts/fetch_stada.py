"""Fetch DB StaDa station master data once and cache it to disk.

Reference data, not event data: it changes on the order of months. Re-run this by
hand when you want a refresh — never on a polling loop.

    set -a; source .env; set +a; uv run python scripts/fetch_stada.py
"""

from __future__ import annotations

import collections
import json
import os
import urllib.request
from pathlib import Path

URL = "https://apis.deutschebahn.com/db-api-marketplace/apis/station-data/v2/stations"
OUT = Path("data/raw/stada/stations.json")


def fetch() -> dict:
    request = urllib.request.Request(
        URL,
        headers={
            "DB-Client-Id": os.environ["STATION_DATA_CLIENT_ID"],
            "DB-Api-Key": os.environ["STATION_DATA_API_KEY"],
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read())


def main() -> None:
    payload = fetch()
    stations = payload["result"]
    print(f"fetched {len(stations)} stations (API reports total={payload.get('total')})")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"cached -> {OUT}  ({OUT.stat().st_size / 1e6:.1f} MB)")

    by_category = collections.Counter(s.get("category") for s in stations)
    print("\nstations by category (1 = biggest hub):")
    running = 0
    for category, count in sorted(by_category.items(), key=lambda kv: (kv[0] is None, kv[0])):
        running += count
        print(f"  category {category}: {count:>5}   cumulative {running:>5}")

    print("\ncategory 1 stations:")
    for station in sorted(
        (s for s in stations if s.get("category") == 1), key=lambda s: s["name"]
    ):
        evas = station.get("evaNumbers", [])
        main = next((e["number"] for e in evas if e.get("isMain")), None)
        print(f"  {station['name']:<34} main_eva={main}  all_evas={[e['number'] for e in evas]}")


if __name__ == "__main__":
    main()
