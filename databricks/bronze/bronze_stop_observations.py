# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — raw stop observations
# MAGIC
# MAGIC Reads JSONL files the bridge uploads to `/Volumes/railway/raw/landing` and appends
# MAGIC them to a Delta table, unchanged.
# MAGIC
# MAGIC **Bronze's only job is durability and fidelity.** Kafka retains 3 days; Bronze
# MAGIC retains everything. No parsing, no filtering, no business logic — so Silver can be
# MAGIC rewritten and replayed against untouched history.
# MAGIC
# MAGIC Each file is one Kafka batch; each line is one Kafka record, with the same columns
# MAGIC `spark.readStream.format("kafka")` would produce.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

CATALOG = "railway"
LANDING = "/Volumes/railway/raw/landing"
# A SEPARATE volume. Not inside `landing`, or the file stream would try to read its
# own checkpoint files as input. Volume paths are /Volumes/<catalog>/<schema>/<volume>,
# so this needs `checkpoints` to exist as a volume — created in the next cell.
CHECKPOINT = "/Volumes/railway/raw/checkpoints/bronze_stop_observations"
TABLE = f"{CATALOG}.bronze.stop_observations"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema is declared, never inferred
# MAGIC
# MAGIC A single stray file with a different shape once added four null columns to every
# MAGIC row via schema inference — silently, and only visible as nulls in a much later
# MAGIC query. Declaring the schema makes a bad file fail loudly instead.

# COMMAND ----------

KAFKA_RECORD = StructType([
    StructField("topic", StringType(), nullable=False),
    StructField("partition", IntegerType(), nullable=False),
    StructField("offset", LongType(), nullable=False),
    StructField("timestamp", LongType(), nullable=True),   # Kafka record time, epoch ms
    StructField("key", StringType(), nullable=True),
    StructField("value", StringType(), nullable=False),    # the payload, UNPARSED
    # Must be declared in the schema, or Spark DROPS malformed lines instead of
    # capturing them. Bronze must never silently lose bytes it cannot parse.
    StructField("_corrupt_record", StringType(), nullable=True),
])

# COMMAND ----------

stream = (
    spark.readStream
    .format("json")
    .schema(KAFKA_RECORD)
    # PERMISSIVE (the default) puts unparseable lines in _corrupt_record instead of
    # dropping them or failing the stream — but only because the column is declared above.
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .load(LANDING)
    # Lineage: which file each row came from, and when we durably stored it. Together
    # with (partition, offset) this traces any Bronze row back to its Kafka record.
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_bronze_ingested_at", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `availableNow`, not a continuous stream
# MAGIC
# MAGIC Free Edition serverless does not support `ProcessingTime` or `Continuous` triggers.
# MAGIC `availableNow` reads everything present, writes it, and stops — which suits hourly
# MAGIC data far better than an always-on stream that would idle 99% of the time and burn
# MAGIC quota. The checkpoint makes the next run resume exactly where this one finished.

# COMMAND ----------

query = (
    stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABLE)
)
query.awaitTermination()

# COMMAND ----------

print(f"rows in {TABLE}: {spark.table(TABLE).count()}")
display(
    spark.table(TABLE)
    .select("topic", "partition", "offset", "key", "_source_file", "_bronze_ingested_at")
    .orderBy("partition", "offset")
    .limit(5)
)
