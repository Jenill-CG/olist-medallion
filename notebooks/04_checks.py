# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 04_checks - automatic data checks after every pipeline run
# MAGIC
# MAGIC **Idea:** every check is a SQL query that returns **one number = how many problems it found**.
# MAGIC
# MAGIC - number <= `max_allowed` -> **PASS**
# MAGIC - otherwise -> **FAIL**
# MAGIC
# MAGIC | Severity | If it fails |
# MAGIC |---|---|
# MAGIC | `critical` | the notebook raises an error -> the Job turns red -> you get the failure email |
# MAGIC | `warning`  | recorded and shown, but the Job stays green |
# MAGIC
# MAGIC Every result is appended to `olist.ops.check_results`, so you keep a history of every run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Setup

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

RESULTS_TABLE = "olist.ops.check_results"
RUN_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
  run_id       STRING,
  check_name   STRING,
  severity     STRING  COMMENT 'critical | warning',
  value        DOUBLE  COMMENT 'number of problems found (or minutes, for freshness)',
  max_allowed  DOUBLE,
  status       STRING  COMMENT 'PASS | FAIL',
  checked_at   TIMESTAMP
) COMMENT 'History of automatic data checks (one row per check per run)'
""")

results = []   # we collect one row per check here


def run_check(check_name, severity, sql, max_allowed=0):
    """Run one check query (it must return ONE number) and record PASS/FAIL."""
    value = spark.sql(sql).collect()[0][0]
    value = float(value) if value is not None else 0.0
    status = "PASS" if value <= max_allowed else "FAIL"
    results.append((RUN_ID, check_name, severity, value, float(max_allowed), status, datetime.utcnow()))
    print(f"{status:4}  {severity:8}  {check_name:45} value={value:g}  (allowed <= {max_allowed})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Bronze: nothing from the source is missing
# MAGIC
# MAGIC We only compare source rows up to the newest change bronze has already seen.
# MAGIC Rows the replay added *after* the bronze run are not "missing" - they will come in the next run.

# COMMAND ----------

run_check("bronze: source orders missing", "critical", """
    SELECT count(*)
    FROM olist_src.app.orders s
    LEFT ANTI JOIN olist.bronze.orders b ON s.order_id = b.order_id
    WHERE s.updated_at <= (SELECT max(updated_at) FROM olist.bronze.orders)
""")

run_check("bronze: source order_items missing", "critical", """
    SELECT count(*)
    FROM olist_src.app.order_items s
    LEFT ANTI JOIN olist.bronze.order_items b
      ON s.order_id = b.order_id AND s.order_item_id = b.order_item_id
    WHERE s.updated_at <= (SELECT max(updated_at) FROM olist.bronze.order_items)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Silver: one row per key, one current version per master

# COMMAND ----------

run_check("silver: duplicate order_id", "critical", """
    SELECT count(*) FROM (
        SELECT order_id FROM olist.silver.orders GROUP BY order_id HAVING count(*) > 1)
""")

run_check("silver: duplicate order item keys", "critical", """
    SELECT count(*) FROM (
        SELECT order_id, order_item_id FROM olist.silver.order_items
        GROUP BY order_id, order_item_id HAVING count(*) > 1)
""")

run_check("silver: sellers with != 1 current version", "critical", """
    SELECT count(*) FROM (
        SELECT seller_id FROM olist.silver.sellers
        GROUP BY seller_id HAVING sum(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)
""")

run_check("silver: products with != 1 current version", "critical", """
    SELECT count(*) FROM (
        SELECT product_id FROM olist.silver.products
        GROUP BY product_id HAVING sum(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)
""")

run_check("silver: customers with != 1 current version", "critical", """
    SELECT count(*) FROM (
        SELECT customer_id FROM olist.silver.customers
        GROUP BY customer_id HAVING sum(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)
""")

# Our TEST_BAD_STATUS_001 row is expected here, so we allow 1.
run_check("silver: rows in quarantine_orders", "warning", """
    SELECT count(*) FROM olist.silver.quarantine_orders
""", max_allowed=1)

# Items whose product or seller doesn't exist in silver (late-arriving masters) - watch, don't fail.
run_check("silver: order items with unknown product", "warning", """
    SELECT count(*)
    FROM olist.silver.order_items i
    LEFT ANTI JOIN (SELECT DISTINCT product_id FROM olist.silver.products) p ON i.product_id = p.product_id
""")

run_check("silver: order items with unknown seller", "warning", """
    SELECT count(*)
    FROM olist.silver.order_items i
    LEFT ANTI JOIN (SELECT DISTINCT seller_id FROM olist.silver.sellers) s ON i.seller_id = s.seller_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Gold: totals reconcile, every sale found its dimensions

# COMMAND ----------

run_check("gold: fact rows vs silver items (difference)", "critical", """
    SELECT abs((SELECT count(*) FROM olist.gold.fact_sales)
             - (SELECT count(*) FROM olist.silver.order_items))
""")

run_check("gold: revenue vs silver (difference R$)", "critical", """
    SELECT abs((SELECT coalesce(sum(price), 0) FROM olist.gold.fact_sales)
             - (SELECT coalesce(sum(price), 0) FROM olist.silver.order_items))
""")

run_check("gold: facts with Unknown (-1) keys", "warning", """
    SELECT count(*) FROM olist.gold.fact_sales
    WHERE customer_key = -1 OR seller_key = -1 OR product_key = -1 OR date_key = -1
""")

run_check("gold: fact dates missing from dim_date", "warning", """
    SELECT count(*)
    FROM olist.gold.fact_sales f
    LEFT ANTI JOIN olist.gold.dim_date d ON f.date_key = d.date_key
""")

run_check("gold: duplicate seller_key in dim_seller", "critical", """
    SELECT count(*) FROM (
        SELECT seller_key FROM olist.gold.dim_seller GROUP BY seller_key HAVING count(*) > 1)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Freshness
# MAGIC
# MAGIC Minutes between the newest change we have seen and the last gold build.
# MAGIC During working hours this should be well under 2 hours. Overnight it grows (the replay pauses), so this is only a warning.

# COMMAND ----------

run_check("freshness: gold lag (minutes)", "warning", """
    SELECT lag_minutes FROM olist.ops.pipeline_freshness
""", max_allowed=120)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Save results and fail the Job if a critical check failed

# COMMAND ----------

schema = StructType([
    StructField("run_id", StringType()),
    StructField("check_name", StringType()),
    StructField("severity", StringType()),
    StructField("value", DoubleType()),
    StructField("max_allowed", DoubleType()),
    StructField("status", StringType()),
    StructField("checked_at", TimestampType()),
])
results_df = spark.createDataFrame(results, schema)
results_df.write.mode("append").saveAsTable(RESULTS_TABLE)
display(results_df)

failed_critical = []
for row in results:
    check_name, severity, status = row[1], row[2], row[5]
    if severity == "critical" and status == "FAIL":
        failed_critical.append(check_name)

if failed_critical:
    raise Exception(f"Critical data checks failed: {failed_critical}")
else:
    print("All critical checks passed.")