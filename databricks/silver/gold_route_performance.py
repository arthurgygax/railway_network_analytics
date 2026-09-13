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
from pyspark.sql.functions import (
    array,
    col,
    concat,
    count,
    countDistinct,
    element_at,
    expr,
    lag,
    lit,
    row_number,
    sum as spark_sum,
    when,
)
from pyspark.sql.window import Window

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
    CONCAT(
      COALESCE(path_from, ARRAY()), 
      ARRAY(station_name), 
      COALESCE(path_to, ARRAY())
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
      ORDER BY SIZE(COALESCE(full_route_raw, ARRAY())) DESC
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
  ELEMENT_AT(full_route, 1) AS origin,
  ELEMENT_AT(full_route, -1) AS destination,
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