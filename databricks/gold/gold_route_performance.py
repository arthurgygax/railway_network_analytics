# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Gold — Route Performance Analytics
# MAGIC
# MAGIC Three tables built from `railway.silver.stop_observations`:
# MAGIC - **route_station_performance**: Stop-level grain — one row per observed (trip, station)
# MAGIC - **route_trip_performance**: Trip-level grain — one row per observed journey
# MAGIC - **route_daily_performance**: Daily route grain — aggregated metrics per (date, origin, destination)
# MAGIC
# MAGIC All tables follow the five rules: collapse observations to last per stop_id, preserve NULLs, derive routes from path arrays, don't rely on train_filter, and separate cancellations from delays.

# COMMAND ----------

# DBTITLE 1,Setup

CATALOG = "railway"
SOURCE = f"{CATALOG}.silver.stop_observations"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------

# DBTITLE 1,A. route_station_performance — Stop-level grain
# Rule 1: Collapse to LAST observation per stop_id
# Rule 3: Build full route from path arrays, with NULL handling and trip-level propagation
station_perf = spark.sql(f"""
WITH last_obs AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY stop_id ORDER BY observed_at DESC) AS rn
  FROM {SOURCE}
),
base AS (
  SELECT 
    -- Trip and train identity
    trip_id,
    start_at,
    train_category,
    train_number,
    train_operator,
    COALESCE(brand, train_operator) AS train_brand,
    
    -- Station and position
    station_eva,
    station_name,
    stop_index,
    
    -- Rule 3: Full route with NULL-safe concatenation
    -- COALESCE path arrays to empty arrays so CONCAT never returns NULL
    -- COALESCE so a NULL path (origin/terminus stop) does not NULL the whole route,
    -- and FILTER so an empty string never becomes a "station" -> ["", "Fulda", ""].
    FILTER(
      CONCAT(
        COALESCE(path_from, ARRAY()),
        ARRAY(station_name),
        COALESCE(path_to, ARRAY())
      ),
      x -> x IS NOT NULL AND TRIM(x) <> ''
    ) AS full_route_raw,
    
    -- Timestamps
    planned_arrival_at,
    changed_arrival_at,
    planned_departure_at,
    changed_departure_at,
    
    -- Rule 2: Preserve NULLs — they mean "no time reported", not "on time"
    arrival_delay_minutes,
    departure_delay_minutes,
    
    -- Rule 5: Cancellations tracked separately
    is_cancelled,
    
    -- Metadata
    DATE(COALESCE(planned_departure_at, planned_arrival_at)) AS service_date,
    observed_at
  FROM last_obs
  WHERE rn = 1  -- Rule 1: Only the final observation per stop_id
),
with_propagated_routes AS (
  SELECT
    *,
    -- Trip-level propagation: fill missing routes from the longest route in the same trip
    FIRST_VALUE(full_route_raw) OVER (
      PARTITION BY trip_id, start_at
      -- Rank by USABLE length: a 1-element route means "no route info", so it must
      -- never outrank a real one. Ranking by raw size let stubs win.
      ORDER BY CASE WHEN SIZE(full_route_raw) >= 2 THEN SIZE(full_route_raw) ELSE 0 END DESC
    ) AS full_route,
    
    -- Rule 4: train_filter is unreliable — use train_category for mode classification
    train_category IN ('ICE','IC','EC','ECE','EN','NJ','RJ','RJX','TGV','FLX','TRI','D') AS is_long_distance
  FROM base
)
SELECT 
  trip_id,
  start_at,
  train_category,
  train_number,
  train_operator,
  train_brand,
  station_eva,
  station_name,
  stop_index,
  full_route,
  -- A one-element route is "we have no route for this stop", not a journey from a
  -- station to itself. Taking element 1 and -1 unconditionally produced 24,106 rows
  -- of "München Hbf -> München Hbf".
  CASE WHEN SIZE(full_route) >= 2 THEN ELEMENT_AT(full_route, 1) END AS origin,
  CASE WHEN SIZE(full_route) >= 2 THEN ELEMENT_AT(full_route, -1) END AS destination,
  planned_arrival_at,
  changed_arrival_at,
  planned_departure_at,
  changed_departure_at,
  arrival_delay_minutes,
  departure_delay_minutes,
  is_cancelled,
  service_date,
  observed_at,
  is_long_distance,
  -- Delay change vs previous OBSERVED stop (not previous scheduled stop — we don't poll all stations)
  COALESCE(departure_delay_minutes, arrival_delay_minutes) - 
    LAG(COALESCE(departure_delay_minutes, arrival_delay_minutes)) 
      OVER (PARTITION BY trip_id, start_at ORDER BY stop_index)
    AS delay_change_vs_previous_observed_stop
FROM with_propagated_routes
""")

station_perf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.route_station_performance")
print(f"✓ Created {CATALOG}.gold.route_station_performance")
print(f"  Rows: {station_perf.count():,}")

# COMMAND ----------

# DBTITLE 1,B. route_trip_performance — Trip-level grain
# Aggregate to trip level — first and last OBSERVED stops
trip_perf = spark.sql(f"""
WITH trip_endpoints AS (
  SELECT
    trip_id,
    start_at,
    train_category,
    train_number,
    train_operator,
    train_brand,
    origin,
    destination,
    service_date,
    is_long_distance,
    
    -- First OBSERVED stop (not necessarily the true origin)
    FIRST_VALUE(station_name) OVER trip_window AS first_observed_station,
    FIRST_VALUE(planned_departure_at) OVER trip_window AS first_observed_planned_departure,
    FIRST_VALUE(changed_departure_at) OVER trip_window AS first_observed_actual_departure,
    FIRST_VALUE(departure_delay_minutes) OVER trip_window AS departure_delay_at_first_observed,
    
    -- Last OBSERVED stop (not necessarily the true destination)
    LAST_VALUE(station_name) OVER trip_window AS last_observed_station,
    LAST_VALUE(planned_arrival_at) OVER trip_window AS last_observed_planned_arrival,
    LAST_VALUE(changed_arrival_at) OVER trip_window AS last_observed_actual_arrival,
    LAST_VALUE(arrival_delay_minutes) OVER trip_window AS arrival_delay_at_last_observed,
    
    COUNT(*) OVER trip_window AS observed_stop_count,
    MAX(CASE WHEN is_cancelled THEN 1 ELSE 0 END) OVER trip_window AS any_stop_cancelled,
    
    ROW_NUMBER() OVER (PARTITION BY trip_id, start_at ORDER BY stop_index) AS rn
  FROM {CATALOG}.gold.route_station_performance
  WINDOW trip_window AS (PARTITION BY trip_id, start_at ORDER BY stop_index
                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT
  trip_id,
  start_at,
  train_category,
  train_number,
  train_operator,
  train_brand,
  origin,
  destination,
  service_date,
  is_long_distance,
  
  -- Observed endpoints with clear labeling
  first_observed_station,
  first_observed_planned_departure,
  first_observed_actual_departure,
  departure_delay_at_first_observed,
  
  last_observed_station,
  last_observed_planned_arrival,
  last_observed_actual_arrival,
  arrival_delay_at_last_observed,
  
  -- Delay progression over the observed span
  arrival_delay_at_last_observed - departure_delay_at_first_observed AS delay_change_over_observed_span,
  
  observed_stop_count,
  CAST(any_stop_cancelled AS BOOLEAN) AS any_stop_cancelled
FROM trip_endpoints
WHERE rn = 1  -- One row per trip
""")

trip_perf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.route_trip_performance")
print(f"✓ Created {CATALOG}.gold.route_trip_performance")
print(f"  Rows: {trip_perf.count():,}")

# COMMAND ----------

# DBTITLE 1,C. route_daily_performance — Daily route grain (long-distance only)
# Rule 2: Exclude NULLs from delay aggregates — they're not zeros
# Rule 5: Exclude cancellations from delay stats, count them separately
# Long-distance only — regional trains kept in route_trip_performance but filtered here
daily_perf = spark.sql(f"""
SELECT
  service_date,
  origin,
  destination,
  
  -- Sample size — ALWAYS show this so readers know when n=1
  COUNT(*) AS trips_observed,
  
  -- Delay distribution (arrival delay at last observed stop, non-cancelled only)
  -- Rule 2: WHERE clause excludes NULLs — they're "no data", not "on time"
  AVG(arrival_delay_at_last_observed) AS avg_arrival_delay_minutes,
  PERCENTILE_APPROX(arrival_delay_at_last_observed, 0.5) AS median_arrival_delay_minutes,
  PERCENTILE_APPROX(arrival_delay_at_last_observed, 0.90) AS p90_arrival_delay_minutes,
  PERCENTILE_APPROX(arrival_delay_at_last_observed, 0.95) AS p95_arrival_delay_minutes,
  
  -- Punctuality buckets
  SUM(CASE WHEN arrival_delay_at_last_observed BETWEEN -999 AND 5 THEN 1 ELSE 0 END) * 100.0 / 
    NULLIF(COUNT(arrival_delay_at_last_observed), 0) AS pct_within_5_min,
  SUM(CASE WHEN arrival_delay_at_last_observed > 15 THEN 1 ELSE 0 END) * 100.0 / 
    NULLIF(COUNT(arrival_delay_at_last_observed), 0) AS pct_over_15_min,
  
  -- Rule 5: Cancellations counted separately, not mixed with delays
  SUM(CASE WHEN any_stop_cancelled THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS cancellation_rate
FROM {CATALOG}.gold.route_trip_performance
WHERE is_long_distance = TRUE  -- Long-distance trains only
GROUP BY service_date, origin, destination
ORDER BY service_date DESC, trips_observed DESC
""")

daily_perf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.route_daily_performance")
print(f"✓ Created {CATALOG}.gold.route_daily_performance")
print(f"  Rows: {daily_perf.count():,}")

# COMMAND ----------

# DBTITLE 1,Route coverage report
# MAGIC %sql
# MAGIC -- Report route coverage improvement
# MAGIC WITH before_fix AS (
# MAGIC   SELECT 
# MAGIC     ELEMENT_AT(CONCAT(path_from, ARRAY(station_name), path_to), 1) AS origin
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY stop_id ORDER BY observed_at DESC) AS rn
# MAGIC     FROM railway.silver.stop_observations
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Before fix (concat with NULLs)' AS scenario,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN origin IS NULL THEN 1 ELSE 0 END) AS null_origins,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN origin IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_with_route
# MAGIC FROM before_fix
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'After fix (with propagation)' AS scenario,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN origin IS NULL THEN 1 ELSE 0 END) AS null_origins,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN origin IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_with_route
# MAGIC FROM railway.gold.route_station_performance

# COMMAND ----------

# DBTITLE 1,Long-distance routes — top 20 by trip count
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   origin, 
# MAGIC   destination, 
# MAGIC   COUNT(*) AS trips, 
# MAGIC   ROUND(AVG(arrival_delay_at_last_observed), 1) AS avg_delay
# MAGIC FROM railway.gold.route_trip_performance
# MAGIC WHERE is_long_distance AND origin IS NOT NULL
# MAGIC GROUP BY 1, 2 
# MAGIC HAVING COUNT(*) >= 3 
# MAGIC ORDER BY trips DESC 
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Validation checks
# MAGIC %sql
# MAGIC -- Check 1: Grain collapsed — must return 0
# MAGIC SELECT 
# MAGIC   'Grain check' AS test,
# MAGIC   CAST(COUNT(*) - COUNT(DISTINCT trip_id, start_at, station_name) AS STRING) AS duplicates,
# MAGIC   CASE WHEN COUNT(*) - COUNT(DISTINCT trip_id, start_at, station_name) = 0 
# MAGIC        THEN '✓ PASS' ELSE '✗ FAIL' END AS status
# MAGIC FROM railway.gold.route_station_performance
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check 2: Observations collapsed correctly
# MAGIC SELECT 
# MAGIC   'Observation collapse' AS test,
# MAGIC   CONCAT(
# MAGIC     'Silver: ', (SELECT FORMAT_NUMBER(COUNT(*), 0) FROM railway.silver.stop_observations), ' obs, ',
# MAGIC     (SELECT FORMAT_NUMBER(COUNT(DISTINCT stop_id), 0) FROM railway.silver.stop_observations), ' stops → ',
# MAGIC     'Gold: ', (SELECT FORMAT_NUMBER(COUNT(*), 0) FROM railway.gold.route_station_performance), ' rows'
# MAGIC   ) AS duplicates,
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM railway.gold.route_station_performance) = 
# MAGIC             (SELECT COUNT(DISTINCT stop_id) FROM railway.silver.stop_observations)
# MAGIC        THEN '✓ PASS' ELSE '✗ FAIL' END AS status
# MAGIC        
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check 3: No zero-filled NULLs
# MAGIC SELECT 
# MAGIC   'NULL preservation' AS test,
# MAGIC   CAST(COUNT(*) AS STRING) AS duplicates,
# MAGIC   CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
# MAGIC FROM railway.gold.route_station_performance
# MAGIC WHERE changed_arrival_at IS NULL AND arrival_delay_minutes IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Sample routes — verify origins/destinations look realistic
# MAGIC %sql
# MAGIC -- Top 20 routes by trip count
# MAGIC SELECT 
# MAGIC   origin,
# MAGIC   destination,
# MAGIC   COUNT(*) AS trips,
# MAGIC   ROUND(AVG(observed_stop_count), 1) AS avg_stops_observed,
# MAGIC   ROUND(AVG(arrival_delay_at_last_observed), 1) AS avg_delay_min
# MAGIC FROM railway.gold.route_trip_performance
# MAGIC GROUP BY origin, destination
# MAGIC ORDER BY trips DESC
# MAGIC LIMIT 20
# COMMAND ----------

# MAGIC %md
# MAGIC # Station dimension
# MAGIC
# MAGIC Coordinates for the map. Uploaded from the laptop by `scripts/upload_station_dim.py`
# MAGIC into a **separate** volume — a dimension file under `landing` would be swept up by
# MAGIC Bronze's file stream and land in `_corrupt_record`.
# MAGIC
# MAGIC 58 of 60 coordinates come from StaDa; the two Swiss stations are added by hand,
# MAGIC because StaDa covers German stations only. `coordinate_source` records which.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE railway.gold.station AS
SELECT station_eva, station_name, stada_name, latitude, longitude,
       coordinate_source, category, federal_state, route_calls
FROM json.`/Volumes/railway/raw/reference/stations.jsonl`
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # segment_performance — the map layer
# MAGIC
# MAGIC **Grain: one row per (from_station, to_station, hops).**
# MAGIC
# MAGIC `hops` is the gap in `stop_index` between two stops we OBSERVED. We only poll ~60
# MAGIC stations, so `hops > 1` means the train called at stations in between that we never
# MAGIC saw — ICE 106 runs Freiburg -> Karlsruhe skipping Baden-Baden, for example.
# MAGIC
# MAGIC **Filter the map to `hops = 1`** for genuine adjacent track sections. Do NOT divide a
# MAGIC multi-hop delay across its sub-sections: we have no idea where in the gap it happened,
# MAGIC and splitting it would invent precision the data does not contain.
# MAGIC
# MAGIC Cancelled stops are excluded from delay maths — a cancelled train has no arrival
# MAGIC time, and counting it as 0 would make punctuality improve every time one is dropped.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE railway.gold.segment_performance AS
WITH consecutive AS (
  SELECT
    LAG(station_name)          OVER w AS from_station,
    station_name                       AS to_station,
    LAG(station_eva)           OVER w AS from_eva,
    station_eva                        AS to_eva,
    stop_index - LAG(stop_index) OVER w AS hops,
    arrival_delay_minutes - LAG(arrival_delay_minutes) OVER w AS delay_gained,
    LAG(arrival_delay_minutes) OVER w AS delay_at_from,
    arrival_delay_minutes              AS delay_at_to,
    (UNIX_TIMESTAMP(planned_arrival_at)
     - UNIX_TIMESTAMP(LAG(planned_departure_at) OVER w)) / 60 AS planned_minutes,
    train_category, service_date,
    COALESCE(is_cancelled, FALSE) OR COALESCE(LAG(is_cancelled) OVER w, FALSE)
      AS either_cancelled
  FROM railway.gold.route_station_performance
  WHERE is_long_distance
  WINDOW w AS (PARTITION BY trip_id, start_at ORDER BY stop_index)
)
SELECT
  c.from_station, c.to_station, c.hops,
  COUNT(*)                                             AS trips,
  ROUND(AVG(c.delay_gained), 2)                        AS avg_delay_gained_minutes,
  ROUND(PERCENTILE_APPROX(c.delay_gained, 0.5), 1)     AS median_delay_gained_minutes,
  ROUND(PERCENTILE_APPROX(c.delay_gained, 0.9), 1)     AS p90_delay_gained_minutes,
  ROUND(AVG(c.delay_at_from), 1)                       AS avg_delay_at_from,
  ROUND(AVG(c.delay_at_to), 1)                         AS avg_delay_at_to,
  ROUND(AVG(c.planned_minutes), 1)                     AS avg_planned_minutes,
  SUM(IF(c.delay_gained > 0, 1, 0))                    AS trips_losing_time,
  SUM(IF(c.delay_gained < 0, 1, 0))                    AS trips_recovering_time,
  f.latitude AS from_latitude, f.longitude AS from_longitude,
  t.latitude AS to_latitude,   t.longitude AS to_longitude
FROM consecutive c
LEFT JOIN railway.gold.station f ON f.station_name = c.from_station
LEFT JOIN railway.gold.station t ON t.station_name = c.to_station
WHERE c.from_station IS NOT NULL
  AND c.delay_gained IS NOT NULL      -- NULL means a time was never reported
  AND NOT c.either_cancelled          -- a cancelled stop has no delay to attribute
GROUP BY c.from_station, c.to_station, c.hops,
         f.latitude, f.longitude, t.latitude, t.longitude
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # station_pair_performance — "how late will I be from A to B?"
# MAGIC
# MAGIC **Grain: one row per (from_station, to_station).** Direct trains only: the self-join
# MAGIC requires the same `trip_id`, so a connection is never counted as one journey.
# MAGIC
# MAGIC Unlike `segment_performance` this is not restricted to adjacent stops — any A before
# MAGIC B on the same trip qualifies, which is what a traveller actually asks about.
# MAGIC
# MAGIC `trips` is on every row on purpose: with ~1 week of data many pairs have single-digit
# MAGIC samples, and a P90 over 3 trains is noise wearing a statistic's clothes.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE railway.gold.station_pair_performance AS
WITH journeys AS (
  SELECT
    a.station_name AS from_station,
    b.station_name AS to_station,
    a.train_category, a.train_number, a.service_date,
    a.departure_delay_minutes AS departure_delay_at_from,
    b.arrival_delay_minutes   AS arrival_delay_at_to,
    b.arrival_delay_minutes - a.departure_delay_minutes AS delay_gained,
    (UNIX_TIMESTAMP(b.planned_arrival_at) - UNIX_TIMESTAMP(a.planned_departure_at)) / 60
      AS planned_journey_minutes,
    b.stop_index - a.stop_index AS stops_between,
    COALESCE(a.is_cancelled, FALSE) OR COALESCE(b.is_cancelled, FALSE) AS either_cancelled
  FROM railway.gold.route_station_performance a
  JOIN railway.gold.route_station_performance b
    ON  a.trip_id = b.trip_id
    AND a.start_at = b.start_at
    AND a.stop_index < b.stop_index      -- direction matters; A must precede B
  WHERE a.is_long_distance AND b.is_long_distance
)
SELECT
  from_station, to_station,
  COUNT(*)                                                    AS direct_trains,
  COUNT(DISTINCT service_date)                                AS days_observed,
  ROUND(AVG(planned_journey_minutes), 0)                      AS avg_planned_journey_minutes,
  ROUND(AVG(arrival_delay_at_to), 1)                          AS avg_arrival_delay_minutes,
  ROUND(PERCENTILE_APPROX(arrival_delay_at_to, 0.5), 1)       AS median_arrival_delay_minutes,
  ROUND(PERCENTILE_APPROX(arrival_delay_at_to, 0.9), 1)       AS p90_arrival_delay_minutes,
  ROUND(PERCENTILE_APPROX(arrival_delay_at_to, 0.95), 1)      AS p95_arrival_delay_minutes,
  ROUND(AVG(delay_gained), 1)                                 AS avg_delay_gained_minutes,
  ROUND(100.0 * SUM(IF(arrival_delay_at_to <= 5, 1, 0)) / COUNT(*), 1)  AS pct_within_5_min,
  ROUND(100.0 * SUM(IF(arrival_delay_at_to > 15, 1, 0)) / COUNT(*), 1)  AS pct_over_15_min,
  ROUND(AVG(stops_between), 1)                                AS avg_stops_between,
  COUNT(DISTINCT train_number)                                AS distinct_services
FROM journeys
WHERE arrival_delay_at_to IS NOT NULL
  AND departure_delay_at_from IS NOT NULL
  AND NOT either_cancelled
GROUP BY from_station, to_station
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks — run these, do not skip them
# MAGIC
# MAGIC `still_circular = 0` is the assertion that catches the "München Hbf -> München Hbf"
# MAGIC bug. A one-element route is missing data, not a journey from a station to itself.

# COMMAND ----------

display(spark.sql("""
SELECT
  SUM(IF(origin IS NULL, 1, 0))                                     AS null_origin,
  SUM(IF(origin IS NOT NULL AND origin = destination, 1, 0))        AS still_circular,
  ROUND(100.0*SUM(IF(origin IS NOT NULL,1,0))/COUNT(*), 1)          AS pct_with_route,
  ROUND(100.0*SUM(IF(is_long_distance AND origin IS NOT NULL,1,0))
        / NULLIF(SUM(IF(is_long_distance,1,0)),0), 1)               AS pct_with_route_long_distance
FROM railway.gold.route_station_performance
"""))

# COMMAND ----------

display(spark.sql("""
SELECT from_station, to_station, trips,
       avg_delay_gained_minutes, avg_planned_minutes
FROM railway.gold.segment_performance
WHERE hops = 1 AND trips >= 5
ORDER BY avg_delay_gained_minutes DESC LIMIT 15
"""))

# COMMAND ----------

display(spark.sql("""
SELECT from_station, to_station, direct_trains, avg_arrival_delay_minutes,
       median_arrival_delay_minutes, p90_arrival_delay_minutes,
       pct_within_5_min, avg_planned_journey_minutes
FROM railway.gold.station_pair_performance
WHERE direct_trains >= 5
ORDER BY direct_trains DESC LIMIT 15
"""))
