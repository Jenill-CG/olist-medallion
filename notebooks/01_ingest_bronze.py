# Databricks notebook source
# 01_ingest_bronze: copy new/changed rows from RDS (olist_src) into olist.bronze
import uuid
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

SRC_CATALOG = "olist_src"            # live window into RDS
BRONZE      = "olist.bronze"         # where Delta copies go
WM_TABLE    = "olist.ops.watermarks"
LOG_TABLE   = "olist.ops.run_log"
OVERLAP     = timedelta(minutes=5)   # safety margin for late commits

TABLES = [
    # (source table,           load mode)
    ("app.orders",             "incremental"),
    ("app.order_items",        "incremental"),
    ("app.order_payments",     "incremental"),
    ("app.order_reviews",      "incremental"),
    ("app.customers",          "incremental"),
    ("app.sellers",            "incremental"),
    ("app.products",           "incremental"),
    ("ref.ref_order_status",     "snapshot"),
    ("ref.ref_payment_type",     "snapshot"),
    ("ref.ref_state_region",     "snapshot"),
    ("ref.ref_product_category", "snapshot"),
    ("ref.ref_geolocation",      "snapshot"),
]

BATCH_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
print("Batch:", BATCH_ID)

# COMMAND ----------

def get_watermark(src):
    """Return last loaded updated_at for a table, or None on first run."""
    rows = spark.sql(f"SELECT last_value FROM {WM_TABLE} WHERE source_table = :src",
                     args={"src": src}).collect()
    return rows[0]["last_value"] if rows else None

def set_watermark(src, value, n):
    """Insert or update the bookmark. Called only AFTER a successful write."""
    spark.sql(f"""
        MERGE INTO {WM_TABLE} w
        USING (SELECT :src AS source_table,
                      CAST(:val AS TIMESTAMP) AS last_value,
                      current_timestamp()     AS last_run_at,
                      CAST(:n AS BIGINT)      AS rows_loaded) s
        ON w.source_table = s.source_table
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""",
        args={"src": src, "val": value, "n": n})

LOG_SCHEMA = StructType([
    StructField("batch_id", StringType()),     StructField("source_table", StringType()),
    StructField("load_mode", StringType()),    StructField("window_from", TimestampType()),
    StructField("window_to", TimestampType()), StructField("rows_loaded", LongType()),
    StructField("status", StringType()),       StructField("error_msg", StringType()),
    StructField("started_at", TimestampType()),StructField("finished_at", TimestampType()),
])

def log_run(src, mode, wf, wt, n, status, err, started):
    row = [(BATCH_ID, src, mode, wf, wt, int(n), status, err, started, datetime.utcnow())]
    spark.createDataFrame(row, LOG_SCHEMA).write.mode("append").saveAsTable(LOG_TABLE)

def add_meta(df, src):
    """Add lineage columns to every bronze row."""
    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_batch_id",    F.lit(BATCH_ID))
              .withColumn("_source",      F.lit(src)))

# COMMAND ----------

def load_incremental(src):
    tgt = f"{BRONZE}.{src.split('.')[1]}"                 # app.orders -> olist.bronze.orders
    wm  = get_watermark(src)
    df  = spark.table(f"{SRC_CATALOG}.{src}")
    win_from = None
    if wm is not None:                                     # first run = full load
        win_from = wm - OVERLAP
        df = df.filter(F.col("updated_at") > F.lit(win_from))   # pushed down to RDS

    # 1) Append to bronze (never update/delete)
    add_meta(df, src).write.mode("append").option("mergeSchema", "true").saveAsTable(tgt)

    # 2) Measure what THIS batch wrote (read back from bronze, not RDS, so numbers are consistent)
    st = (spark.table(tgt).filter(F.col("_batch_id") == BATCH_ID)
               .agg(F.count("*").alias("n"), F.max("updated_at").alias("mx")).collect()[0])
    n, mx = st["n"], st["mx"]

    # 3) Move the bookmark only if something was loaded
    if n > 0:
        set_watermark(src, mx, n)
    return win_from, (mx if n > 0 else wm), n


def load_snapshot(src):
    tgt = f"{BRONZE}.{src.split('.')[1]}"
    (add_meta(spark.table(f"{SRC_CATALOG}.{src}"), src)
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tgt))
    return None, None, spark.table(tgt).count()

# COMMAND ----------

results, failed = [], []
for src, mode in TABLES:
    started = datetime.utcnow()
    try:
        wf, wt, n = load_incremental(src) if mode == "incremental" else load_snapshot(src)
        status, err = "success", None
    except Exception as e:
        wf = wt = None; n = 0
        status, err = "failed", str(e)[:1000]
        failed.append(src)
    log_run(src, mode, wf, wt, n, status, err, started)
    results.append((src, mode, int(n), status))
    print(f"{status:8} {mode:12} {src:28} rows={n}")

display(spark.createDataFrame(results, "source_table string, mode string, rows long, status string"))

if failed:   # makes the scheduled Job show red if anything failed
    raise Exception(f"Bronze load failed for: {failed}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze vs source (bronze may be slightly behind if a replay tick happened since)
# MAGIC SELECT 'orders' t,
# MAGIC (SELECT count(*) FROM olist.bronze.orders)    bronze, 
# MAGIC (SELECT count(*) FROM olist_src.app.orders)    source
# MAGIC UNION ALL 
# MAGIC SELECT 'order_items', (SELECT count(*) FROM olist.bronze.order_items), 
# MAGIC (SELECT count(*) FROM olist_src.app.order_items)
# MAGIC UNION ALL 
# MAGIC SELECT 'sellers',
# MAGIC (SELECT count(*) FROM olist.bronze.sellers),     
# MAGIC (SELECT count(*) FROM olist_src.app.sellers)
# MAGIC UNION ALL 
# MAGIC SELECT 'products',
# MAGIC (SELECT count(*) FROM olist.bronze.products),
# MAGIC (SELECT count(*) FROM olist_src.app.products);
# MAGIC
# MAGIC SELECT * FROM olist.ops.watermarks ORDER BY source_table;   -- 7 rows (fewer if a table was still empty)
# MAGIC SELECT * FROM olist.ops.run_log    ORDER BY started_at;     -- 12 rows, all success

# COMMAND ----------



# COMMAND ----------

