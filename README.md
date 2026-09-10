# railway-network-analytics

Ingestion and analytics for German long-distance rail, built on the
[Deutsche Bahn Timetables and StaDa APIs](https://developers.deutschebahn.com).

**Status: ingestion working, Kafka not yet wired.** What exists today is a Dockerised
service that polls DB's APIs hourly and writes JSON Lines. Kafka, Databricks and the
Bronze/Silver/Gold layers are *planned*, not built — see
[Planned architecture](#planned-architecture).

---

## What the ingestion service does

Each cycle, for each of ~60 stations:

1. **Fills a plan cache.** `plan/{eva}/{date}/{hour}` is the scheduled skeleton — train
   identity (`ICE 575`), planned times, planned platform, full route path. Past hours are
   immutable, so they are cached on disk and fetched once.
2. **Fetches changes.** `fchg/{eva}` returns every currently-known change for that
   station over a rolling ~28 h window.
3. **Joins them** on the stop `id`, which decomposes as
   `{trip_id}-{start_datetime}-{stop_index}`.
4. **Scopes to long-distance** via `<tl f="F">`.
5. **Suppresses unchanged records** against the previous cycle, using a persistent
   content-hash file.
6. **Writes** the remainder as JSON Lines, one file per UTC day.

### Implemented architecture

```
  DB StaDa API                        DB Timetables API
  (5,408 stations)                    60 req/min, free tier
        |                                    |
        | one-off, cached                    | hourly
        v                                    v
  scripts/fetch_stada.py            +------------------+     +------------------+
  scripts/pick_poll_targets.py      |  plan/{eva}/...  |     |   fchg/{eva}     |
        |                           |  the skeleton    |     |  the changes     |
        v                           +------------------+     +------------------+
  data/raw/stada/                            |                        |
    poll_targets.json  ------------------>  cached on disk            |
    (60 stations)                            |                        |
                                             +-----------+------------+
                                                         | join on stop id
                                                         v
                                              +---------------------------+
                                              |   DbTimetablesSource      |
                                              |   -> [StopObservation]    |
                                              +---------------------------+
                                                         |
                                                         v
                                              +---------------------------+
                                              |   ChangeFilter            |
                                              |   drop what didn't change |
                                              +---------------------------+
                                                         |
                                                         v
                                              +---------------------------+
                                              |   JsonLinesSink           |
                                              |   data/out/*.jsonl        |
                                              +---------------------------+
```

### Output record

```json
{
  "stop_id": "3704713885565394549-2609101024-9",
  "station_eva": 8070003,
  "station_name": "Frankfurt(M) Flughafen Fernbf",
  "trip_id": "3704713885565394549",
  "start_datetime": "2609101024",
  "stop_index": 9,
  "train_category": "ICE", "train_number": "29",
  "train_operator": "80", "train_filter": "F",
  "planned_arrival": "2609101359", "changed_arrival": "2609101407",
  "planned_departure": "2609101402", "changed_departure": "2609101409",
  "planned_platform": "Fern 5", "changed_platform": null,
  "planned_path": "Frankfurt(Main)Hbf|Hanau Hbf|Würzburg Hbf|Nürnberg Hbf",
  "changed_path": null,
  "messages": [["d", "43", null]],
  "observed_at": "2026-09-10T12:27:07+02:00"
}
```

Natural key: `stop_id`. Timestamps are `YYMMDDHHMM` in Europe/Berlin local time, exactly
as the API gives them. Delay is `changed − planned`, a derived value that belongs
downstream rather than in ingestion.

---

## Running it locally

Requires [uv](https://docs.astral.sh/uv/), Python 3.14, and DB API credentials from
[developers.deutschebahn.com](https://developers.deutschebahn.com) (free tier).

```bash
uv sync && cp .env.example .env    # then fill in the four credentials
```

Generate the station scope — a one-off, roughly 5 minutes of rate-limited requests:

```bash
set -a; source .env; set +a; uv run python scripts/fetch_stada.py && uv run python scripts/pick_poll_targets.py
```

Run one cycle:

```bash
set -a; source .env; set +a; MAX_POLLS=1 uv run python -m railway_network_analytics
```

Run hourly until `Ctrl-C`:

```bash
set -a; source .env; set +a; uv run python -m railway_network_analytics
```

## Running it with Docker

```bash
docker build -t railway-ingest:0.2 .
```

Credentials are injected, never baked in. `STATE_DIR` must be a volume — the plan cache
and change-detection state have to survive between cycles:

```bash
docker run --rm -e TIMETABLES_CLIENT_ID -e TIMETABLES_API_KEY -e MAX_POLLS=1 -v rna-state:/app/data/state -v rna-out:/app/data/out railway-ingest:0.2
```

Inspect what it wrote:

```bash
docker run --rm -v rna-out:/d alpine sh -c 'wc -l /d/*.jsonl && head -1 /d/*.jsonl'
```

`docker stop` sends `SIGTERM`; the service finishes cleanly and logs its final counters.

## Configuration

Environment variables, read once at startup and validated together. Invalid config exits
with code `2` listing *every* problem. Secrets are redacted from the config log by field
name (`password`, `secret`, `token`, `key`).

| Variable | Default | Notes |
|---|---|---|
| `TIMETABLES_CLIENT_ID` | — | Required |
| `TIMETABLES_API_KEY` | — | Required, **secret** |
| `STATION_DATA_CLIENT_ID` / `_API_KEY` | — | Only needed by `scripts/fetch_stada.py` |
| `POLL_TARGETS_PATH` | `data/raw/stada/poll_targets.json` | Must exist |
| `STATE_DIR` | `data/state` | Plan cache + change state. **Must persist.** |
| `OUTPUT_DIR` | `data/out` | Checked writable at startup |
| `POLL_INTERVAL_SECONDS` | `3600` | Fixed-rate, not fixed-delay |
| `HTTP_TIMEOUT_SECONDS` | `60` | |
| `MAX_POLLS` | `0` | `0` = run until stopped |
| `LOG_LEVEL` / `LOG_FORMAT` | `INFO` / `text` | `json` in the container |
| `SINK` | `jsonl` | `kafka` exists but is not yet the real path |

### Exit codes

`0` clean shutdown · `2` invalid configuration · `3` poll targets missing/empty ·
`4` output destination not writable

### Rate-limit arithmetic

The free tier allows **60 requests/minute**. Measured steady-state cost:

```
60 stations x (2 plan + 1 fchg) = 180 requests/cycle
= 3.3 min wall clock, 3 req/min, 5% of budget, 4,320 requests/day of 86,400
```

The plan cache does most of that work: a cold cycle costs 18 plan requests per station,
a warm one costs 2.

## Testing

```bash
uv run pytest ; uv run ruff check .
```

68 tests, fully offline — synthetic XML fixtures and a stubbed HTTP client. Covers XML
parsing, the plan/change join, scope filtering, the plan cache, change detection and its
persistence, failure modes, config validation and exit codes.

---

## Data-source notes

Measured facts that constrain what the analytics layers can produce.

- **Station-centric, not network-centric.** You request per station, so the station scope
  is a real design decision. All 5,408 stations hourly would exceed the rate limit.
- **Scope is derived, not guessed.** StaDa's `category` measures station size, *not*
  long-distance service — category 1 is only 23 stations and excludes Münster, Fulda,
  Bielefeld, Göttingen and Bremen. `scripts/pick_poll_targets.py` instead reads the
  `ppth` route paths of real trains to find which stations Fernverkehr actually serves.
- **The canonical EVA is not always the right one.** Berlin Hbf's `8011160` returns **0**
  planned stops; its Fernverkehr is on `8098160`. Sibling EVAs `8089xxx`/`8098xxx` are
  usually S-Bahn platforms with zero long-distance traffic.
- **Join on EVA, never on name.** Timetables says `Frankfurt(Main)Hbf`, StaDa says
  `Frankfurt (Main) Hbf`.
- **`fchg` is a sparse delta.** Only 32 of 778 stops carry `<tl>`, so it cannot identify a
  train on its own — hence the plan join.
- **~14 % of changes stay unidentified**, because their plan hour fell outside the
  17-hour window. These are kept deliberately: they skew towards trains that started long
  ago, i.e. the badly delayed ones. Scope filtering excludes what is positively
  identified as regional, not everything unconfirmed.
- **Delay distribution** over one sample: min −5, median +4, p95 +45, max +126 minutes.

## Known limitations

- **Hourly granularity.** `fchg` reports each train's current delay at *every* stop, so
  spatial evolution along a route is fully captured. What hourly gives up is *temporal*
  evolution — watching one estimate move minute by minute. That would need `rchg` at
  ~90 s, costing 68 % of the request budget instead of 5 %.
- **At-least-once.** Change state is replaced each cycle, so a stop that leaves the feed
  and returns unchanged is re-emitted once. Downstream must be idempotent.
- **No retry backoff.** A failed station is skipped; the next cycle is the retry. Cheap,
  because `fchg`'s 28 h window means a missed cycle self-heals.
- **Real hourly suppression is unmeasured.** Back-to-back cycles suppress ~96 %, but that
  is not a fair test of an hour's gap.
- **Message codes are opaque.** `("d", "43")`, `cat="Störung"` — DB publishes a code
  table; decoding belongs in Silver.

---

## Planned architecture

Not implemented. Recorded so the direction is explicit, not so it looks finished.

```
[implemented]                                  [planned]
DB APIs --> ingestion --> JSONL          --> Kafka --> Databricks --> Bronze --> Silver --> Gold
                          (Kafka sink                                              |
                           exists but is                                   station/route
                           not yet the                                     enrichment
                           real path)
```

Aiven Kafka connection parameters for the next phase: `SASL_SSL`, `SCRAM-SHA-256`, CA
cert at `certs/ca.pem` (gitignored), credentials via `KAFKA_BOOTSTRAP_SERVERS`,
`KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_CA_CERT`.

## Repository layout

```
src/railway_network_analytics/
    config.py          Environment -> validated Config. The only reader of os.environ.
    logging_setup.py   Text/JSON formatters. Called once, from the entrypoint.
    db_timetables.py   Source adapter: fetch, parse XML, join plan+changes, scope.
    change_filter.py   Persistent content-hash filter. Source-agnostic.
    sink.py            Output boundary: ObservationSink protocol + JSON Lines writer.
    kafka_sink.py      Same protocol, Kafka. Not yet the default path.
    service.py         Poll scheduling and failure policy.
    __main__.py        Entrypoint: config, logging, signals, wiring, exit codes.
tests/                 Offline and deterministic. No network, no credentials.
scripts/
    fetch_stada.py            One-off: cache station master data.
    pick_poll_targets.py      One-off: derive and verify the station scope.
    probe_timetables_join.py  Diagnostic: inspect the plan/changes join.
    profile_gtfs_rt.py        Phase 0 analysis of the retired GTFS.de source.
data/                  Source data, state and output. Gitignored.
```

## Attribution

Data from the Deutsche Bahn Timetables and StaDa APIs, © DB InfraGO AG.
