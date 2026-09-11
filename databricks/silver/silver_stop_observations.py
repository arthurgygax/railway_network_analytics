# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — parsed stop observations
# MAGIC
# MAGIC **Grain: one row per _observation_** — `(stop_id, observed_at)` — not one row per stop.
# MAGIC
# MAGIC That is deliberate. The source publishes a *forecast that converges on an actual*:
# MAGIC one stop was observed at `+24 → +25 → +23` minutes. Those are not three
# MAGIC measurements, they are two predictions and an outcome. Deciding which one counts
# MAGIC is a business question, so Silver keeps the whole series and **Gold** collapses it.
# MAGIC
# MAGIC Silver does: parse, type, normalize timestamps, compute delays, flag cancellations,
# MAGIC deduplicate. It does **not** interpret.

# COMMAND ----------

from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    from_json,
    lit,
    split,
    to_timestamp,
    to_utc_timestamp,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CATALOG = "railway"
SOURCE = f"{CATALOG}.bronze.stop_observations"
TABLE = f"{CATALOG}.silver.stop_observations"
CHECKPOINT = "/Volumes/railway/raw/checkpoints/silver_stop_observations"
BERLIN = "Europe/Berlin"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The payload schema
# MAGIC
# MAGIC Mirrors the producer's `StopObservation`. Declared explicitly: a field the producer
# MAGIC adds later arrives as a new column only when we add it here, which is the point —
# MAGIC Bronze keeps the raw string, so nothing is lost in the meantime.

# COMMAND ----------

PAYLOAD = StructType([
    StructField("stop_id", StringType()),
    StructField("station_eva", LongType()),
    StructField("station_name", StringType()),
    StructField("trip_id", StringType()),
    StructField("start_datetime", StringType()),
    StructField("stop_index", IntegerType()),
    StructField("train_category", StringType()),
    StructField("train_number", StringType()),
    StructField("train_operator", StringType()),
    StructField("train_filter", StringType()),
    StructField("planned_arrival", StringType()),
    StructField("changed_arrival", StringType()),
    StructField("planned_departure", StringType()),
    StructField("changed_departure", StringType()),
    StructField("planned_arrival_platform", StringType()),
    StructField("planned_departure_platform", StringType()),
    StructField("changed_arrival_platform", StringType()),
    StructField("changed_departure_platform", StringType()),
    StructField("planned_path_from", StringType()),
    StructField("planned_path_to", StringType()),
    StructField("changed_path_from", StringType()),
    StructField("changed_path_to", StringType()),
    StructField("arrival_status", StringType()),
    StructField("departure_status", StringType()),
    StructField("cancelled_at", StringType()),
    StructField("brand", StringType()),
    StructField("messages", ArrayType(ArrayType(StringType()))),
    StructField("observed_at", StringType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Timestamps
# MAGIC
# MAGIC DB gives `YYMMDDHHMM` in **Europe/Berlin local time**. Two traps: the century is
# MAGIC missing, and Berlin is UTC+1 or UTC+2 depending on the date. `to_utc_timestamp`
# MAGIC handles DST properly — a fixed offset would be silently wrong for half the year.

# COMMAND ----------


def db_timestamp(column):
    """YYMMDDHHMM in Berlin local time -> UTC timestamp."""
    return to_utc_timestamp(
        to_timestamp(concat(lit("20"), column), "yyyyMMddHHmm"), BERLIN
    )


def delay_minutes(planned, changed):
    """changed - planned, in minutes.

    NULL when either side is missing — "no time reported" is not "on time". Conflating
    them is the single easiest way to make a punctuality metric quietly optimistic.
    """
    return when(
        planned.isNull() | changed.isNull(), lit(None).cast("int")
    ).otherwise(((changed.cast("long") - planned.cast("long")) / 60).cast("int"))


# COMMAND ----------

parsed = (
    spark.readStream.table(SOURCE)
    .withColumn("obs", from_json(col("value"), PAYLOAD))
    .select(
        # Kafka + Bronze lineage: every Silver row traces back to its exact source record.
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("_source_file"),
        col("_bronze_ingested_at"),
        col("obs.*"),
    )
)

# COMMAND ----------

silver = (
    parsed
    .withColumn("observed_at", to_timestamp(col("observed_at")))
    .withColumn("start_at", db_timestamp(col("start_datetime")))
    .withColumn("planned_arrival_at", db_timestamp(col("planned_arrival")))
    .withColumn("changed_arrival_at", db_timestamp(col("changed_arrival")))
    .withColumn("planned_departure_at", db_timestamp(col("planned_departure")))
    .withColumn("changed_departure_at", db_timestamp(col("changed_departure")))
    .withColumn("cancelled_at", db_timestamp(col("cancelled_at")))
    .withColumn(
        "arrival_delay_minutes",
        delay_minutes(col("planned_arrival_at"), col("changed_arrival_at")),
    )
    .withColumn(
        "departure_delay_minutes",
        delay_minutes(col("planned_departure_at"), col("changed_departure_at")),
    )
    # The only cancellation signal the feed carries. Either leg being cancelled means
    # this stop did not happen as planned.
    .withColumn(
        "is_cancelled",
        (col("arrival_status") == "c") | (col("departure_status") == "c"),
    )
    # Routes as arrays, so "does this trip pass through X" is array_contains(...)
    # instead of a LIKE on a pipe-delimited string.
    .withColumn("path_from", split(coalesce(col("changed_path_from"),
                                            col("planned_path_from")), r"\|"))
    .withColumn("path_to", split(coalesce(col("changed_path_to"),
                                          col("planned_path_to")), r"\|"))
    .drop("planned_arrival", "changed_arrival", "planned_departure", "changed_departure",
          "start_datetime", "planned_path_from", "planned_path_to",
          "changed_path_from", "changed_path_to")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication
# MAGIC
# MAGIC `(stop_id, observed_at)` is the natural key of an observation. Duplicates should be
# MAGIC impossible already — Kafka offsets are unique, the bridge names files
# MAGIC deterministically, and Bronze's checkpoint processes each file once — so this is
# MAGIC defence in depth against Kafka's at-least-once guarantee.
# MAGIC
# MAGIC The watermark is what makes it safe in a stream: without one, the dedup state set
# MAGIC would grow forever. Two days comfortably exceeds Kafka's 3-day retention window
# MAGIC minus our hourly cadence.

# COMMAND ----------

deduplicated = (
    silver
    .withWatermark("observed_at", "2 days")
    .dropDuplicates(["stop_id", "observed_at"])
)

# COMMAND ----------

query = (
    deduplicated.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TABLE)
)
query.awaitTermination()

# COMMAND ----------

print(f"rows: {spark.table(TABLE).count()}")
display(
    spark.table(TABLE)
    .select("train_category", "train_number", "station_name", "stop_index",
            "planned_arrival_at", "changed_arrival_at", "arrival_delay_minutes",
            "is_cancelled", "path_to", "observed_at")
    .orderBy(col("observed_at").desc())
    .limit(10)
)
