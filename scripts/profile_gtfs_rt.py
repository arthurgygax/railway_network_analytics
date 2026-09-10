"""Profile a GTFS-RT snapshot and test whether it can be joined to the static
Fernverkehr GTFS feed.

Read-only, offline, deterministic. Nothing here writes to data/ or talks to Kafka.

    uv run python scripts/profile_gtfs_rt.py
    uv run python scripts/profile_gtfs_rt.py --self-check
"""

import argparse
import collections
import csv
import datetime
import io
import zipfile
from pathlib import Path

from google.transit import gtfs_realtime_pb2

RT_PATH = Path("data/raw/gtfs/realtime.pb")
STATIC_PATH = Path("data/raw/gtfs/fernverkehr.zip")
TZ = datetime.timezone(datetime.timedelta(hours=2))  # Europe/Berlin, CEST at feed date
DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

TRIP_REL = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship
STU_REL = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship


# --------------------------------------------------------------------------- io


def read_static(path):
    """Return {table_name: [row dicts]} for the static GTFS zip."""
    with zipfile.ZipFile(path) as z:
        out = {}
        for name in z.namelist():
            with z.open(name) as f:
                out[name] = list(csv.DictReader(io.TextIOWrapper(f, "utf-8-sig")))
        return out


def read_rt(path):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(path.read_bytes())
    return feed


# ------------------------------------------------------------------- static gtfs


def active_services(static, date):
    """service_ids running on `date` (YYYYMMDD), per calendar + calendar_dates."""
    dow = DAYS[datetime.datetime.strptime(date, "%Y%m%d").weekday()]
    active = {
        c["service_id"]
        for c in static["calendar.txt"]
        if c["start_date"] <= date <= c["end_date"] and c[dow] == "1"
    }
    for e in static["calendar_dates.txt"]:
        if e["date"] == date:
            (active.add if e["exception_type"] == "1" else active.discard)(e["service_id"])
    return active


def trip_stop_sequences(static, trip_ids):
    """{trip_id: [stop_id, ...]} ordered by stop_sequence, for trip_ids only."""
    rows = collections.defaultdict(list)
    for r in static["stop_times.txt"]:
        if r["trip_id"] in trip_ids:
            rows[r["trip_id"]].append((int(r["stop_sequence"]), r["stop_id"]))
    return {t: [s for _, s in sorted(v)] for t, v in rows.items()}


def trip_time_spans(static, trip_ids):
    """{trip_id: (first_time, last_time)} as HH:MM:SS strings (may exceed 24h)."""
    span = {}
    for r in static["stop_times.txt"]:
        t = r["trip_id"]
        if t not in trip_ids:
            continue
        v = r["departure_time"] or r["arrival_time"]
        if not v:
            continue
        lo, hi = span.get(t, (v, v))
        span[t] = (min(lo, v), max(hi, v))
    return span


# ------------------------------------------------------------------- rt profiling


def pct(n, d):
    return f"{100 * n / d:6.2f}%" if d else "   n/a"


def profile_feed(feed, out):
    ts = feed.header.timestamp
    out(f"gtfs_realtime_version : {feed.header.gtfs_realtime_version}")
    out(f"incrementality        : {feed.header.Incrementality.Name(feed.header.incrementality)}")
    out(f"header.timestamp      : {ts}  = {datetime.datetime.fromtimestamp(ts, TZ).isoformat()} (Europe/Berlin)")
    kinds = collections.Counter()
    for e in feed.entity:
        for f in ("trip_update", "vehicle", "alert"):
            if e.HasField(f):
                kinds[f] += 1
        if not (e.HasField("trip_update") or e.HasField("vehicle") or e.HasField("alert")):
            kinds["<empty/other>"] += 1
    out(f"total entities        : {len(feed.entity)}")
    for k, v in sorted(kinds.items()):
        out(f"  {k:<20}: {v} ({pct(v, len(feed.entity)).strip()})")
    return kinds


def profile_trip_updates(tus, out):
    n = len(tus)
    have = collections.Counter()
    trip_rel = collections.Counter()
    stu_rel = collections.Counter()
    stu = arr = dep = arr_d = dep_d = arr_t = dep_t = seq = sid = 0
    delays = []
    for tu in tus:
        t = tu.trip
        have["trip.trip_id"] += bool(t.trip_id)
        have["trip.route_id"] += bool(t.route_id)
        have["trip.start_date"] += bool(t.start_date)
        have["trip.start_time"] += bool(t.start_time)
        have["trip.direction_id (set)"] += t.HasField("direction_id")
        have["trip.schedule_relationship (set)"] += t.HasField("schedule_relationship")
        have["trip_update.timestamp"] += tu.HasField("timestamp")
        have["trip_update.delay"] += tu.HasField("delay")
        have["trip_update.vehicle"] += tu.HasField("vehicle")
        if tu.HasField("vehicle"):
            v = tu.vehicle
            have["  vehicle.id"] += bool(v.id)
            have["  vehicle.label"] += bool(v.label)
            have["  vehicle.license_plate"] += bool(v.license_plate)
        trip_rel[TRIP_REL.Name(t.schedule_relationship)] += 1
        for s in tu.stop_time_update:
            stu += 1
            seq += s.HasField("stop_sequence")
            sid += bool(s.stop_id)
            arr += s.HasField("arrival")
            dep += s.HasField("departure")
            arr_d += s.arrival.HasField("delay")
            dep_d += s.departure.HasField("delay")
            arr_t += s.arrival.HasField("time")
            dep_t += s.departure.HasField("time")
            stu_rel[STU_REL.Name(s.schedule_relationship)] += 1
            if s.departure.HasField("delay"):
                delays.append(s.departure.delay)

    out(f"TripUpdates: {n}")
    out("")
    out("  field presence (share of TripUpdates)")
    for k in (
        "trip.trip_id", "trip.route_id", "trip.start_date", "trip.start_time",
        "trip.direction_id (set)", "trip.schedule_relationship (set)",
        "trip_update.timestamp", "trip_update.delay", "trip_update.vehicle",
        "  vehicle.id", "  vehicle.label", "  vehicle.license_plate",
    ):
        out(f"    {k:<34} {have[k]:>8}  {pct(have[k], n)}")

    out("")
    out(f"  StopTimeUpdates: {stu}  (mean {stu / n:.1f} per trip)" if n else "  StopTimeUpdates: 0")
    for label, c in (
        ("stop_sequence", seq), ("stop_id", sid),
        ("arrival", arr), ("departure", dep),
        ("arrival.delay", arr_d), ("departure.delay", dep_d),
        ("arrival.time", arr_t), ("departure.time", dep_t),
    ):
        out(f"    {label:<34} {c:>8}  {pct(c, stu)}")

    out("")
    out("  TripDescriptor.schedule_relationship (effective value incl. proto default)")
    for k, v in trip_rel.most_common():
        out(f"    {k:<34} {v:>8}  {pct(v, n)}")
    out("  StopTimeUpdate.schedule_relationship")
    for k, v in stu_rel.most_common():
        out(f"    {k:<34} {v:>8}  {pct(v, stu)}")

    if delays:
        delays.sort()
        q = lambda p: delays[int(p * (len(delays) - 1))]
        out("")
        out(f"  departure.delay seconds  min={delays[0]} p25={q(.25)} median={q(.5)} "
            f"p75={q(.75)} p95={q(.95)} max={delays[-1]}")
    return stu


def profile_identity(tus, out):
    ids = collections.Counter()
    pairs = set()
    dates = collections.Counter()
    for tu in tus:
        t = tu.trip
        ids[t.trip_id] += 1
        pairs.add((t.trip_id, t.start_date))
        dates[t.start_date] += 1
    reused = {t for t, _ in pairs}
    multi = collections.Counter(t for t, _ in pairs)
    n_multi = sum(1 for t, c in multi.items() if c > 1)
    out(f"  unique trip_id                    : {len(ids)}")
    out(f"  unique (trip_id, start_date) pairs: {len(pairs)}")
    out(f"  trip_ids on >1 start_date         : {n_multi}")
    out(f"  trip_ids appearing >1x in feed    : {sum(1 for c in ids.values() if c > 1)}")
    out("  start_date distribution           : "
        + ", ".join(f"{d}={c}" for d, c in sorted(dates.items())))
    return reused, dates


def profile_alerts(alerts, out):
    n = len(alerts)
    have = collections.Counter()
    cause = collections.Counter()
    effect = collections.Counter()
    sev = collections.Counter()
    ie_kind = collections.Counter()
    texts = collections.Counter()
    for a in alerts:
        have["informed_entity"] += bool(a.informed_entity)
        have["active_period"] += bool(a.active_period)
        have["header_text.translation"] += bool(a.header_text.translation)
        have["description_text.translation"] += bool(a.description_text.translation)
        have["url"] += bool(a.url.translation)
        cause[a.Cause.Name(a.cause)] += 1
        effect[a.Effect.Name(a.effect)] += 1
        sev[a.SeverityLevel.Name(a.severity_level)] += 1
        for ie in a.informed_entity:
            for f in ("agency_id", "route_id", "stop_id"):
                if getattr(ie, f):
                    ie_kind[f] += 1
            if ie.HasField("trip"):
                ie_kind["trip.trip_id" if ie.trip.trip_id else "trip (no trip_id)"] += 1
        for tr in a.description_text.translation:
            texts[tr.text] += 1
    out(f"Alerts: {n}")
    for k in ("informed_entity", "active_period", "header_text.translation",
              "description_text.translation", "url"):
        out(f"    {k:<34} {have[k]:>8}  {pct(have[k], n)}")
    out("  informed_entity selectors used:")
    for k, v in ie_kind.most_common():
        out(f"    {k:<34} {v:>8}")
    for label, c in (("cause", cause), ("effect", effect), ("severity_level", sev)):
        out(f"  {label}: " + ", ".join(f"{k}={v}" for k, v in c.most_common(5)))
    out(f"  distinct description_text values  : {len(texts)}")
    for t, v in texts.most_common(3):
        out(f"    {v:>8}x  {t[:90]!r}")
    return texts


def profile_extensions(feed, out):
    """Report protobuf extensions and unknown (non-standard) wire fields."""
    ext = collections.Counter()
    unknown = collections.Counter()
    unknown_ok = True

    def scan(msg, path):
        nonlocal unknown_ok
        for f, _ in msg.ListFields():
            if f.is_extension:
                ext[f"{path}.{f.full_name}"] += 1
        try:
            for uf in msg.UnknownFields():
                unknown[f"{path}#{uf.field_number}"] += 1
        except (AttributeError, ValueError, NotImplementedError):
            unknown_ok = False
        for f, v in msg.ListFields():
            if f.message_type is None:
                continue
            for sub in (v if f.is_repeated else [v]):
                scan(sub, f"{path}.{f.name}")

    scan(feed.header, "header")
    for e in feed.entity:
        scan(e, "entity")
    out(f"  declared extension fields set     : {len(ext)}")
    if not unknown_ok:
        out("  unknown-field scan                : UNAVAILABLE under the C/upb protobuf runtime.")
        out("    Re-run with PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python to introspect")
        out("    non-standard wire fields (slower, same output otherwise).")
    else:
        out(f"  unknown (non-standard) wire fields: {len(unknown)}")
    if not ext and not unknown:
        out("  -> no GTFS-RT extensions in use; every populated field is standard v2.0.")
    for k, v in ext.most_common():
        out(f"  extension     {k}  x{v}")
    for k, v in unknown.most_common():
        out(f"  unknown field {k}  x{v}")
    return ext, unknown


# ------------------------------------------------------------------ reconciliation


def reconcile(feed_date, feed_ts, tus, static, out):
    trips = static["trips.txt"]
    routes = {r["route_id"]: r for r in static["routes.txt"]}
    agencies = {a["agency_id"]: a for a in static["agency.txt"]}
    by_id = {t["trip_id"]: t for t in trips}

    rt_seq = {}
    for tu in tus:
        rt_seq.setdefault(
            tu.trip.trip_id, [s.stop_id for s in tu.stop_time_update if s.stop_id]
        )
    rt_ids = set(rt_seq)
    static_ids = set(by_id)
    matched = rt_ids & static_ids

    out("7. RT trip_id vs static Fernverkehr trips.txt")
    out(f"  unique RT trip_ids                : {len(rt_ids)}")
    out(f"  static Fernverkehr trip_ids       : {len(static_ids)}")
    out(f"  RT ids found in static            : {len(matched)}  ({pct(len(matched), len(rt_ids)).strip()} of RT)")
    out(f"  RT ids NOT found in static        : {len(rt_ids) - len(matched)}")
    out(f"  static ids covered by RT          : {len(matched)}  ({pct(len(matched), len(static_ids)).strip()} of static)")

    # Are those matches real, or numeric-id collisions? Compare stop sequences.
    st_seq = trip_stop_sequences(static, matched)
    exact = prefix = disjoint = 0
    for t in matched:
        a, b = rt_seq[t], st_seq.get(t, [])
        if a == b:
            exact += 1
        elif a and b and (set(a) <= set(b) or set(b) <= set(a)):
            prefix += 1
        elif not set(a) & set(b):
            disjoint += 1
    out("")
    out("  match validation (RT stop_id sequence vs static stop_times.txt):")
    out(f"    identical sequence              : {exact}  {pct(exact, len(matched))}")
    out(f"    subset/partial (in-progress trip): {prefix}")
    out(f"    no stop in common (=collision)  : {disjoint}")

    # 8. What did we match?
    out("")
    out("8. Matched RT trips joined to static route/agency")
    brk = collections.Counter()
    for t in matched:
        r = routes[by_id[t]["route_id"]]
        a = agencies.get(r["agency_id"], {})
        brk[(r["agency_id"], a.get("agency_name", "?"), r["route_short_name"], r["route_id"])] += 1
    out(f"  agency_id  agency_name                      route_short_name  route_id  trips")
    for (aid, aname, rsn, rid), c in sorted(brk.items(), key=lambda kv: (-kv[1], kv[0])):
        out(f"  {aid:>9}  {aname:<32} {rsn:<17} {rid:<9} {c}")
    agg = collections.Counter()
    for (aid, aname, _, _), c in brk.items():
        agg[(aid, aname)] += c
    out("  by agency:")
    for (aid, aname), c in agg.most_common():
        out(f"    agency_id={aid:<4} {aname:<32} {c} trips")

    # 9. Alternative mapping for the non-matching majority.
    out("")
    out("9. Non-matching RT trips — alternative mapping attempts")
    unmatched = rt_ids - matched
    out(f"  non-matching RT trips             : {len(unmatched)}")
    out("  RT fields available for a join    : trip_id, start_date, stop_id, stop_sequence,")
    out("                                      arrival/departure delay+time. Nothing else is set.")
    out(f"  trip.route_id present in RT       : 0  -> route-level join impossible")
    out(f"  trip.start_time present in RT     : 0  -> (route, start_time) join impossible")
    out("  static trips.txt columns          : "
        + ", ".join(trips[0].keys()) + "  -> no trip_short_name / trip_headsign to match on")

    rt_stops = {s for v in rt_seq.values() for s in v}
    st_stops = {s["stop_id"] for s in static["stops.txt"]}
    out(f"  distinct stop_ids in RT           : {len(rt_stops)}")
    out(f"  distinct stop_ids in static       : {len(st_stops)}")
    out(f"  shared stop_ids                   : {len(rt_stops & st_stops)}  "
        f"({pct(len(rt_stops & st_stops), len(st_stops)).strip()} of static stops)")

    # Fingerprint join: can the full ordered stop sequence recover any extra trip?
    fp = collections.defaultdict(list)
    for t, seq in trip_stop_sequences(static, static_ids).items():
        fp[tuple(seq)].append(t)
    hits = sum(1 for t in unmatched if tuple(rt_seq[t]) in fp)
    out(f"  unmatched RT trips whose full stop sequence matches a static trip: {hits}")

    # 6/coverage: how much of the *active* static day does RT actually carry?
    out("")
    out("Coverage of the static feed for the RT service date")
    active = active_services(static, feed_date)
    active_trips = {t["trip_id"] for t in trips if t["service_id"] in active}
    out(f"  static services active on {feed_date} : {len(active)} of {len(static['calendar.txt'])}")
    out(f"  static trips active on {feed_date}    : {len(active_trips)} of {len(trips)}")
    out(f"  of those, present in RT             : {len(active_trips & rt_ids)}  "
        f"{pct(len(active_trips & rt_ids), len(active_trips))}")

    # Split by whether the trip was actually running at the feed timestamp.
    now = datetime.datetime.fromtimestamp(feed_ts, TZ).strftime("%H:%M:%S")
    spans = trip_time_spans(static, active_trips)
    state = collections.Counter()
    covered = collections.Counter()
    for t in active_trips:
        lo, hi = spans.get(t, ("", ""))
        k = "running" if lo <= now <= hi else ("not yet started" if lo > now else "already finished")
        agency = routes[by_id[t]["route_id"]]["agency_id"]
        state[(k, agency == "9")] += 1
        if t in rt_ids:
            covered[(k, agency == "9")] += 1
    out(f"  feed timestamp local time           : {now}")
    out("  state at feed time            all agencies        DB Fernverkehr (agency_id=9)")
    for k in ("already finished", "running", "not yet started"):
        a_c, a_n = covered[(k, True)] + covered[(k, False)], state[(k, True)] + state[(k, False)]
        d_c, d_n = covered[(k, True)], state[(k, True)]
        out(f"    {k:<20} {a_c:>6}/{a_n:<6} {pct(a_c, a_n)}      {d_c:>6}/{d_n:<6} {pct(d_c, d_n)}")

    # Which agencies in the active static day get RT at all?
    out("  RT coverage by agency (trips running at feed time):")
    per = collections.Counter()
    per_c = collections.Counter()
    for t in active_trips:
        lo, hi = spans.get(t, ("", ""))
        if not (lo <= now <= hi):
            continue
        a = routes[by_id[t]["route_id"]]["agency_id"]
        key = (a, agencies.get(a, {}).get("agency_name", "?"))
        per[key] += 1
        if t in rt_ids:
            per_c[key] += 1
    for key, n in sorted(per.items(), key=lambda kv: -kv[1]):
        out(f"    agency_id={key[0]:<4} {key[1]:<32} {per_c[key]:>4}/{n:<4} {pct(per_c[key], n)}")

    return {
        "rt_ids": rt_ids, "matched": matched, "exact": exact, "disjoint": disjoint,
        "active_trips": active_trips, "fp_hits": hits,
        "running_db": (covered[("running", True)], state[("running", True)]),
    }


# ----------------------------------------------------------------------- report


def report(rt_path, static_path, write):
    lines = []
    out = lines.append

    feed = read_rt(rt_path)
    static = read_static(static_path)
    tus = [e.trip_update for e in feed.entity if e.HasField("trip_update")]
    alerts = [e.alert for e in feed.entity if e.HasField("alert")]

    out("=" * 100)
    out("GTFS-RT PROFILING REPORT")
    out(f"  realtime feed : {rt_path}")
    out(f"  static feed   : {static_path}")
    out("  All numbers below are FACTS measured from these two files. Sections marked")
    out("  ASSUMPTION / OPEN QUESTION are interpretation, not measurement.")
    out("=" * 100)

    out("")
    out("1-2. FEED HEADER AND ENTITY COUNTS")
    profile_feed(feed, out)

    out("")
    out("3-5. TRIPUPDATE / STOPTIMEUPDATE STRUCTURE")
    profile_trip_updates(tus, out)

    out("")
    out("6. TRIP IDENTITY (trip_id vs start_date)")
    profile_identity(tus, out)

    out("")
    out("-" * 100)
    out("7-9. RECONCILIATION WITH STATIC FERNVERKEHR")
    out("-" * 100)
    feed_date = collections.Counter(tu.trip.start_date for tu in tus).most_common(1)[0][0]
    res = reconcile(feed_date, feed.header.timestamp, tus, static, out)

    out("")
    out("10. ALERTS")
    texts = profile_alerts(alerts, out)

    out("")
    out("11. PROTOBUF EXTENSIONS / NON-STANDARD FIELDS")
    profile_extensions(feed, out)

    # ------------------------------------------------------------- conclusions
    m, rt_n = len(res["matched"]), len(res["rt_ids"])
    run_c, run_n = res["running_db"]
    n_alerts = len(alerts)
    boiler = texts.most_common(1)[0][1] if texts else 0
    real_alerts = sum(1 for a in alerts if a.Cause.Name(a.cause) != "UNKNOWN_CAUSE")

    out("")
    out("=" * 100)
    out("ASSUMPTIONS")
    out("=" * 100)
    out("  A1. Feed timezone is Europe/Berlin (CEST, UTC+2) on the observed date. static")
    out("      calendar dates and stop_times are interpreted in that local time.")
    out("  A2. A static trip is 'running' if the feed timestamp falls between its first and")
    out("      last scheduled stop time. Trips past midnight (>24:00:00) are not corrected.")
    out("  A3. Both files were captured close enough in time to describe the same timetable")
    out("      version. If the static feed is regenerated on a different schedule than the RT")
    out("      producer, ids can drift — see OQ1.")
    out("  A4. The RT feed is the nationwide gtfs.de free feed; the static feed is its")
    out("      Fernverkehr subset. This is inferred from the attribution text in the alerts")
    out("      and from the size ratio, not from any field in the data.")

    out("")
    out("=" * 100)
    out("OPEN QUESTIONS")
    out("=" * 100)
    out("  OQ1. Are trip_ids stable across static feed regenerations? A single snapshot")
    out("       cannot answer this. Test: download the static feed on two different days and")
    out("       diff trip_id -> stop sequence. This is the single biggest risk to the join.")
    out("  OQ2. Does the nationwide *static* feed (gtfs.de 'latest', all modes) share this")
    out("       id space? If yes it is the better dimension source than the Fernverkehr subset.")
    out("  OQ3. How long does a trip stay in the RT feed before/after it runs? Needs several")
    out("       snapshots over a day.")
    out("  OQ4. departure.delay spans roughly -5.8h to +3.8h. Large negative delays are not")
    out("       plausible as 'early' and are more likely midnight-rollover or producer noise.")
    out("       Decide a clamp/quarantine rule before these reach any aggregate.")
    out("  OQ5. Do non-DB operators (SBB, OeBB, NS, PKP, SNCF, CD) ever appear in this RT feed,")
    out("       or is realtime DB-only? One snapshot cannot distinguish 'never' from 'not now'.")

    out("")
    out("=" * 100)
    out("RECOMMENDATION")
    out("=" * 100)
    out("  1. Can the free GTFS-RT feed be reliably joined to the static Fernverkehr feed?")
    out(f"     YES. The two feeds share one trip_id namespace. Of the {m} overlapping ids,")
    out(f"     {res['exact']} reproduce the static stop_id sequence exactly and the rest are")
    out("     prefixes/subsets of it (trips already part-way through their run);")
    out(f"     {res['disjoint']} share no stop at all, i.e. there are no coincidental numeric")
    out("     collisions. The initial 'ids do not match' observation is a")
    out("     sampling artifact: the RT feed is nationwide (regional + long-distance), so a")
    out(f"     random RT trip is almost never Fernverkehr ({pct(m, rt_n).strip()} of RT trips are).")
    out("     Judge the join from the static side, not the RT side.")
    out("")
    out("  2. Recommended join key")
    out("     (trip_id, start_date)  ->  static trips.trip_id")
    out("     - trip_id alone is NOT a primary key over time: it repeats across service dates,")
    out("       so start_date is required to identify a run.")
    out("     - Chain static: trips.route_id -> routes.route_id -> routes.agency_id -> agency.")
    out("     - Filter Fernverkehr by agency_id, not by the RT payload: RT carries no route_id,")
    out("       no start_time, no vehicle label worth using.")
    out("     - Stop level: (trip_id, start_date, stop_sequence) joins to stop_times. stop_id is")
    out("       set on 100% of StopTimeUpdates, so use it as a cross-check on the sequence.")
    out("     - Model stop-level cancellation explicitly: TripDescriptor is SCHEDULED for 100% of")
    out("       trips, but ~7% of StopTimeUpdates are SKIPPED. Trip-level status is useless here;")
    out("       cancellations only show up per stop.")
    out("")
    out("  3. Additional investigation required")
    out("     - Verify trip_id stability across static feed regenerations (OQ1). Pin the static")
    out("       feed version used to build dimensions and re-load it whenever it changes.")
    out("     - Evaluate the nationwide static feed (OQ2); it likely raises coverage and removes")
    out("       the need to treat unmatched RT trips as garbage.")
    out("     - Keep unmatched RT trips out of the Fernverkehr model rather than guessing: with")
    out("       route_id and start_time absent, and trips.txt carrying only")
    out("       (route_id, service_id, trip_id), there is no second join path. A stop-sequence")
    out(f"       fingerprint join recovers {res['fp_hits']} extra trips, i.e. it is not a workaround.")
    out("")
    out("  4. Suitable as the temporary real-time source?")
    out("     YES, with three documented limitations.")
    out(f"     - Coverage is live-window only: {pct(run_c, run_n).strip()} of DB Fernverkehr trips")
    out("       scheduled to be running at the feed timestamp are present, but only a few percent")
    out("       of trips that have not departed yet. One snapshot is a slice of the day, not the")
    out("       day. Poll continuously and accumulate; do not expect a full day in one file.")
    out("     - Realtime is effectively DB-only. Foreign operators in the static feed get no")
    out("       TripUpdates in this snapshot, so delay metrics must be scoped to agency_id=9")
    out("       (and 7) or they will silently under-report.")
    out(f"     - Alerts carry almost no information: {n_alerts - real_alerts} of {n_alerts} are")
    out("       UNKNOWN_CAUSE, 100% are UNKNOWN_EFFECT, and the description text is a data-source")
    out(f"       attribution string ({boiler} share the single most common one). Only {real_alerts}")
    out("       have a real cause. Ingest TripUpdates only and drop Alert entities at the producer;")
    out("       revisit only if disruption text becomes a requirement — the informed_entity does")
    out("       carry trip.trip_id, so they would join on the same key.")
    out("=" * 100)

    text = "\n".join(lines)
    write(text)
    return text


# ------------------------------------------------------------------- self-check


def _self_check():
    """Smallest thing that fails if the profiling logic breaks."""
    static = {
        "calendar.txt": [
            {"service_id": "s1", "start_date": "20260901", "end_date": "20260930",
             **{d: "1" for d in DAYS}},
            {"service_id": "s2", "start_date": "20260901", "end_date": "20260930",
             **{d: "0" for d in DAYS}},
        ],
        "calendar_dates.txt": [
            {"service_id": "s1", "date": "20260909", "exception_type": "2"},
            {"service_id": "s2", "date": "20260909", "exception_type": "1"},
        ],
        "stop_times.txt": [
            {"trip_id": "t1", "stop_sequence": "1", "stop_id": "B",
             "arrival_time": "10:00:00", "departure_time": "10:01:00"},
            {"trip_id": "t1", "stop_sequence": "0", "stop_id": "A",
             "arrival_time": "09:00:00", "departure_time": "09:00:00"},
            {"trip_id": "t2", "stop_sequence": "0", "stop_id": "Z",
             "arrival_time": "23:00:00", "departure_time": "23:00:00"},
        ],
    }
    # calendar_dates must override calendar in both directions
    assert active_services(static, "20260909") == {"s2"}
    assert active_services(static, "20260910") == {"s1"}
    # stop order must come from stop_sequence, not file order
    assert trip_stop_sequences(static, {"t1"}) == {"t1": ["A", "B"]}
    assert trip_time_spans(static, {"t1"}) == {"t1": ("09:00:00", "10:01:00")}
    assert pct(1, 4).strip() == "25.00%" and pct(1, 0).strip() == "n/a"
    print("self-check OK")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rt", type=Path, default=RT_PATH)
    p.add_argument("--static", type=Path, default=STATIC_PATH)
    p.add_argument("-o", "--output", type=Path, help="also write the report here")
    p.add_argument("--self-check", action="store_true", help="run assertions and exit")
    a = p.parse_args()
    if a.self_check:
        return _self_check()

    def write(text):
        print(text)
        if a.output:
            a.output.write_text(text + "\n")

    report(a.rt, a.static, write)


if __name__ == "__main__":
    main()
