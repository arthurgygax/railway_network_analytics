"""Decide which (station, eva) pairs we poll — derived from data, not from a guess.

Why this is not just "the biggest stations": StaDa's `category` measures station size
and facilities, NOT whether long-distance trains call there. Category 1 is only 23
stations and excludes Münster, Fulda, Bielefeld, Göttingen, Bremen, Osnabrück and
other major ICE stops.

So we derive the Fernverkehr network from the data itself:

  A. Seed with StaDa category-1 stations and read a few hours of `plan` from each.
     Every long-distance train carries `ppth` — its full route as a station list.
     Counting those names tells us which stations Fernverkehr actually serves.
  B. Resolve the top N names to EVA numbers with Timetables' own `/station/` endpoint.
     Never match on name: Timetables says "Frankfurt(Main)Hbf", StaDa says
     "Frankfurt (Main) Hbf", and foreign stations are in neither.
  C. Verify each EVA answers with Fernverkehr. The canonical EVA is not always the
     right one — Berlin Hbf's 8011160 returns 0 stops while 8098160 carries the ICEs —
     so fall back to the station's meta EVAs when the canonical one is empty.

One-off. Re-run when the scope changes, and run it during the day: probing at 03:00
would find almost nothing anywhere. Costs roughly 250 requests / 5 minutes.

    set -a; source .env; set +a; uv run python scripts/pick_poll_targets.py [N]
"""

from __future__ import annotations

import collections
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

STADA = Path("data/raw/stada/stations.json")
OUT = Path("data/raw/stada/poll_targets.json")
API = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"

DEFAULT_STATIONS = 60
SEED_HOURS = 4          # hours of `plan` read from each seed station
RATE_LIMIT_SLEEP = 1.1  # 60 req/min -> stay just under one per second
BERLIN = ZoneInfo("Europe/Berlin")


def _get(path: str) -> ET.Element | None:
    request = urllib.request.Request(
        f"{API}/{path}",
        headers={
            "DB-Client-Id": os.environ["TIMETABLES_CLIENT_ID"],
            "DB-Api-Key": os.environ["TIMETABLES_API_KEY"],
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return ET.fromstring(response.read())
    except (urllib.error.URLError, ET.ParseError, TimeoutError):
        return None
    finally:
        time.sleep(RATE_LIMIT_SLEEP)


def fernverkehr_stops(root: ET.Element | None) -> list[ET.Element]:
    if root is None:
        return []
    return [s for s in root.findall("s")
            if s.find("tl") is not None and s.find("tl").get("f") == "F"]


def discover_network(seed_evas: list[int], now: datetime) -> collections.Counter:
    """Count how often each station name appears on a Fernverkehr route."""
    calls: collections.Counter = collections.Counter()
    for eva in seed_evas:
        for offset in range(SEED_HOURS):
            when = now + timedelta(hours=offset)
            root = _get(f"plan/{eva}/{when:%y%m%d}/{when:%H}")
            for stop in fernverkehr_stops(root):
                for elem in (stop.find("ar"), stop.find("dp")):
                    if elem is None:
                        continue
                    for name in (elem.get("ppth") or "").split("|"):
                        if name:
                            calls[name] += 1
    return calls


def resolve(name: str) -> tuple[int, list[int]] | None:
    """Station name -> (canonical eva, meta evas). Authoritative; no name matching."""
    root = _get(f"station/{urllib.parse.quote(name)}")
    if root is None:
        return None
    for station in root.findall("station"):
        if station.get("db") != "true" or not station.get("eva"):
            continue  # foreign stations (Basel SBB, Amsterdam Centraal) are out of scope
        metas = [int(m) for m in (station.get("meta") or "").split("|") if m.isdigit()]
        return int(station.get("eva")), metas
    return None


def pick_eva(candidates: list[int], now: datetime) -> tuple[int, int] | None:
    """First candidate EVA that reports Fernverkehr in the current hour."""
    for eva in candidates:
        count = len(fernverkehr_stops(_get(f"plan/{eva}/{now:%y%m%d}/{now:%H}")))
        if count:
            return eva, count
    return None


def main() -> None:
    wanted = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_STATIONS
    stations = json.loads(STADA.read_text(encoding="utf-8"))["result"]
    by_eva = {e["number"]: s for s in stations for e in s.get("evaNumbers", [])}
    seed = [next(e["number"] for e in s["evaNumbers"] if e.get("isMain"))
            for s in stations if s.get("category") == 1]
    now = datetime.now(BERLIN)

    print(f"A. discovering the network from {len(seed)} seed stations x {SEED_HOURS}h of plan ...")
    calls = discover_network(seed, now)
    print(f"   {len(calls)} distinct stations appear on Fernverkehr routes\n")

    print(f"B/C. resolving and verifying the top {wanted} ...")
    targets = []
    for name, weight in calls.most_common():
        if len(targets) >= wanted:
            break
        resolved = resolve(name)
        if resolved is None:
            print(f"   skip   {name:<34} (not a DB station)")
            continue
        canonical, metas = resolved
        chosen = pick_eva([canonical, *metas], now)
        if chosen is None:
            print(f"   skip   {name:<34} eva={canonical} no Fernverkehr in probe hour")
            continue
        eva, fern = chosen
        station = by_eva.get(eva, {})
        note = "" if eva == canonical else f" (canonical {canonical} was empty)"
        print(f"   keep   {name:<34} eva={eva} route_calls={weight} probe_F={fern}{note}")
        targets.append({
            "eva": eva,
            "name": name,
            "stada_name": station.get("name"),
            "station_number": station.get("number"),
            "category": station.get("category"),
            "federal_state": station.get("federalState"),
            "route_calls": weight,
        })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(targets, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\n{len(targets)} poll targets -> {OUT}")
    for interval in (60, 90, 120, 180):
        rate = len(targets) * 60 / interval
        flag = "" if rate <= 30 else ("  <-- tight" if rate <= 45 else "  <-- OVER")
        print(f"  every {interval:>3}s = {rate:>4.0f} req/min = {100 * rate / 60:>3.0f}% of budget{flag}")


if __name__ == "__main__":
    main()
