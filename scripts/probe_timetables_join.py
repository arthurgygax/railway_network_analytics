"""Prove the plan-to-changes join on one station before building the adapter.

`plan` is the scheduled skeleton (train identity, planned times, planned route).
`rchg` is the change stream (what actually moved). They share the stop `id`, which
decomposes as {trip_id}-{start_datetime}-{stop_index}.

    set -a; source .env; set +a; uv run python scripts/probe_timetables_join.py 8000105
"""

from __future__ import annotations

import os
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

API = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"
BERLIN = ZoneInfo("Europe/Berlin")


def get(path: str) -> ET.Element:
    request = urllib.request.Request(
        f"{API}/{path}",
        headers={
            "DB-Client-Id": os.environ["TIMETABLES_CLIENT_ID"],
            "DB-Api-Key": os.environ["TIMETABLES_API_KEY"],
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return ET.fromstring(response.read())


def ts(value: str | None) -> str | None:
    """DB timestamps are YYMMDDHHMM. Render readable; keep None as None."""
    if not value:
        return None
    return f"20{value[0:2]}-{value[2:4]}-{value[4:6]} {value[6:8]}:{value[8:10]}"


def main() -> None:
    eva = sys.argv[1] if len(sys.argv) > 1 else "8000105"
    now = datetime.now(BERLIN)

    # Plan: several hours, because a change can reference a train planned hours ago.
    planned: dict[str, ET.Element] = {}
    for offset in range(-1, 3):
        when = now + timedelta(hours=offset)
        for stop in get(f"plan/{eva}/{when:%y%m%d}/{when:%H}").findall("s"):
            planned[stop.get("id")] = stop
    print(f"plan:  {len(planned)} stops across 4 hours")

    changes = get(f"rchg/{eva}")
    changed = changes.findall("s")
    station = changes.get("station")
    print(f"rchg:  {len(changed)} changed stops at {station}\n")

    matched = unmatched = 0
    shown = 0
    for stop in changed:
        stop_id = stop.get("id")
        plan_stop = planned.get(stop_id)
        if plan_stop is None:
            unmatched += 1
            continue
        matched += 1

        tl = plan_stop.find("tl")
        if tl is None or tl.get("f") != "F" or shown >= 8:
            continue
        shown += 1

        trip_id, start, index = stop_id.rsplit("-", 2)
        print(f"{tl.get('c')} {tl.get('n')}   trip={trip_id[:20]} start={ts(start)} stop_index={index}")
        for tag, label in (("ar", "arr"), ("dp", "dep")):
            p, c = plan_stop.find(tag), stop.find(tag)
            if p is None and c is None:
                continue
            pt = ts(p.get("pt")) if p is not None else None
            ct = ts(c.get("ct")) if c is not None else None
            pp = p.get("pp") if p is not None else None
            cp = c.get("cp") if c is not None else None
            delay = ""
            if pt and ct:
                mins = (datetime.strptime(ct, "%Y-%m-%d %H:%M")
                        - datetime.strptime(pt, "%Y-%m-%d %H:%M")).total_seconds() / 60
                delay = f"  delay {mins:+.0f}min"
            plat = f"  platform {pp}->{cp}" if cp and cp != pp else f"  platform {pp}" if pp else ""
            print(f"   {label}  planned {pt}  changed {ct or '-'}{delay}{plat}")
        msgs = [(m.get("t"), m.get("c"), m.get("cat")) for m in stop.iter("m")]
        if msgs:
            print(f"   messages {msgs[:4]}{' ...' if len(msgs) > 4 else ''}")
        print()

    total = matched + unmatched
    print(f"join: {matched}/{total} changed stops matched a planned stop "
          f"({100 * matched / total:.0f}%), {unmatched} unmatched")


if __name__ == "__main__":
    main()
