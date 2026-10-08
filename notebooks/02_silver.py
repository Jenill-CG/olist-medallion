# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver — bronze ➜ silver (simple version)
# MAGIC
# MAGIC **How this notebook works:** every run rebuilds each silver table from **all** of bronze.
# MAGIC No bookmarks, no MERGE. Running it twice gives exactly the same result.
# MAGIC
# MAGIC | Group | Tables | What we do |
# MAGIC |---|---|---|
# MAGIC | Lookups | 5 `ref_*` | copy from bronze, drop technical columns |
# MAGIC | Transactions | orders, order_items, order_payments, order_reviews | keep only the **latest version** of each row |
# MAGIC | Masters | sellers, products, customers | keep the **full change history** (SCD Type 2) |
# MAGIC | Bad rows | `quarantine_<table>` | rows that fail a check, with the reason |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

BRONZE = "olist.bronze"
SILVER = "olist.silver"


def save_table(df, table_name):
    """Write a DataFrame to olist.silver.<table_name>, replacing whatever was there."""
    full_name = f"{SILVER}.{table_name}"
    df = df.withColumn("_silver_loaded_at", F.current_timestamp())
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    print(f"saved {full_name:38} rows = {spark.table(full_name).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Lookup tables (copy as-is)

# COMMAND ----------

lookup_tables = ["ref_order_status", "ref_payment_type", "ref_state_region",
                 "ref_product_category", "ref_geolocation"]

for name in lookup_tables:
    df = spark.table(f"{BRONZE}.{name}")
    df = df.drop("_ingested_at", "_batch_id", "_source")   # bronze-only technical columns
    save_table(df, name)

# COMMAND ----------

# Turn the lookups into plain Python lists. We use them to check the other tables.
valid_statuses = []
for row in spark.table(f"{SILVER}.ref_order_status").collect():
    valid_statuses.append(row["status_code"])

valid_payment_types = []
for row in spark.table(f"{SILVER}.ref_payment_type").collect():
    valid_payment_types.append(row["payment_type"])

valid_states = []
for row in spark.table(f"{SILVER}.ref_state_region").collect():
    valid_states.append(row["state_code"])

print("statuses     :", valid_statuses)
print("payment types:", valid_payment_types)
print("states       :", len(valid_states), "states")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Helper: keep only the latest version of each row
# MAGIC
# MAGIC Bronze has many copies of the same order (created → shipped → delivered, plus overlap re-reads).
# MAGIC This helper numbers the copies of each key from newest to oldest and keeps number 1.

# COMMAND ----------

def keep_latest(df, key_columns):
    newest_first = Window.partitionBy(*key_columns).orderBy(
        F.col("updated_at").desc(),      # newest change first
        F.col("_ingested_at").desc()     # tie-break: most recently loaded copy
    )
    df = df.withColumn("row_num", F.row_number().over(newest_first))
    df = df.filter(F.col("row_num") == 1)
    return df.drop("row_num")


def split_good_bad(df, is_valid, reason):
    """
    is_valid : a condition that is True for good rows
    reason   : a column that says WHY a row is bad
    Returns (good_rows, bad_rows).
    """
    # If a value is NULL the check gives NULL (not True/False). coalesce turns NULL into False = bad.
    df = df.withColumn("is_valid", F.coalesce(is_valid, F.lit(False)))
    good = df.filter(F.col("is_valid") == True).drop("is_valid")
    bad = df.filter(F.col("is_valid") == False).drop("is_valid").withColumn("reason", reason)
    return good, bad

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Orders (latest version)

# COMMAND ----------

orders = spark.table(f"{BRONZE}.orders").drop("_source")
orders = keep_latest(orders, ["order_id"])

is_valid = (
    F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("order_status").isin(valid_statuses)
)
reason = (
    F.when(F.col("order_id").isNull(), "missing order_id")
     .when(F.col("customer_id").isNull(), "missing customer_id")
     .otherwise("unknown order_status")
)

good, bad = split_good_bad(orders, is_valid, reason)
save_table(good, "orders")
save_table(bad, "quarantine_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Order items (latest version)

# COMMAND ----------

items = spark.table(f"{BRONZE}.order_items").drop("_source")
items = keep_latest(items, ["order_id", "order_item_id"])   # one order has items 1, 2, 3 ...

is_valid = (
    F.col("product_id").isNotNull()
    & F.col("seller_id").isNotNull()
    & (F.col("price") >= 0)
    & (F.col("freight_value") >= 0)
)
reason = (
    F.when(F.col("product_id").isNull(), "missing product_id")
     .when(F.col("seller_id").isNull(), "missing seller_id")
     .otherwise("negative price or freight")
)

good, bad = split_good_bad(items, is_valid, reason)
save_table(good, "order_items")
save_table(bad, "quarantine_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Order payments (latest version)

# COMMAND ----------

payments = spark.table(f"{BRONZE}.order_payments").drop("_source")
payments = keep_latest(payments, ["order_id", "payment_sequential"])   # one order can be paid in parts

is_valid = (
    F.col("payment_type").isin(valid_payment_types)
    & (F.col("payment_value") >= 0)
)
reason = (
    F.when(~F.col("payment_type").isin(valid_payment_types), "unknown payment_type")
     .otherwise("negative or missing payment_value")
)

good, bad = split_good_bad(payments, is_valid, reason)
save_table(good, "order_payments")
save_table(bad, "quarantine_order_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Order reviews (latest version)

# COMMAND ----------

reviews = spark.table(f"{BRONZE}.order_reviews").drop("_source")
reviews = keep_latest(reviews, ["review_id", "order_id"])   # review_id alone is not unique in Olist

is_valid = F.col("review_score").between(1, 5)
reason = F.lit("review_score not between 1 and 5")

good, bad = split_good_bad(reviews, is_valid, reason)
save_table(good, "order_reviews")
save_table(bad, "quarantine_order_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Helper: build change history (SCD Type 2)
# MAGIC
# MAGIC Example for one seller in bronze:
# MAGIC
# MAGIC | city | updated_at | note |
# MAGIC |---|---|---|
# MAGIC | campinas | Oct 5 10:00 | first load |
# MAGIC | sorocaba | Oct 6 12:15 | seller moved |
# MAGIC | sorocaba | Oct 6 12:15 | overlap copy |
# MAGIC
# MAGIC Result in silver:
# MAGIC
# MAGIC | version | city | valid_from | valid_to | is_current |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | campinas | Oct 5 10:00 | Oct 6 12:15 | false |
# MAGIC | 2 | sorocaba | Oct 6 12:15 | NULL | true |

# COMMAND ----------

def build_history(df, key_column, tracked_columns):
    # 1) Remove overlap copies: one row per key + updated_at
    df = df.dropDuplicates([key_column, "updated_at"])

    # 2) Make a fingerprint of the columns we care about.
    #    Same fingerprint = nothing important changed.
    parts = []
    for col_name in tracked_columns:
        parts.append(F.coalesce(F.col(col_name).cast("string"), F.lit("NULL")))
    df = df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))

    # 3) Compare each row with the previous version of the same key.
    #    Keep the first version, and every row where the fingerprint changed.
    oldest_first = Window.partitionBy(key_column).orderBy("updated_at")
    df = df.withColumn("prev_hash", F.lag("row_hash").over(oldest_first))
    df = df.filter(F.col("prev_hash").isNull() | (F.col("row_hash") != F.col("prev_hash")))
    df = df.drop("prev_hash")

    # 4) Each version is valid from its own updated_at until the next version's updated_at.
    df = df.withColumn("valid_from", F.col("updated_at"))
    df = df.withColumn("valid_to", F.lead("updated_at").over(oldest_first))   # NULL = still current
    df = df.withColumn("is_current", F.col("valid_to").isNull())
    df = df.withColumn("version", F.row_number().over(oldest_first))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Sellers (history)

# COMMAND ----------

sellers = spark.table(f"{BRONZE}.sellers").drop("_source")
sellers = sellers.withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))   # "  Sao Paulo " -> "sao paulo"

is_valid = F.col("seller_state").isin(valid_states)
reason = F.lit("unknown seller_state")
good, bad = split_good_bad(sellers, is_valid, reason)

history = build_history(good, "seller_id",
                        ["seller_zip_code_prefix", "seller_city", "seller_state"])
save_table(history, "sellers")
save_table(bad, "quarantine_sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Products (history)

# COMMAND ----------

products = spark.table(f"{BRONZE}.products").drop("_source")
# fix the misspelled column names from the Olist dataset
products = products.withColumnRenamed("product_name_lenght", "product_name_length")
products = products.withColumnRenamed("product_description_lenght", "product_description_length")

is_valid = F.col("product_id").isNotNull()
reason = F.lit("missing product_id")
good, bad = split_good_bad(products, is_valid, reason)

history = build_history(good, "product_id",
                        ["product_category_name", "product_name_length", "product_description_length",
                         "product_photos_qty", "product_weight_g", "product_length_cm",
                         "product_height_cm", "product_width_cm"])
save_table(history, "products")
save_table(bad, "quarantine_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Customers (history)

# COMMAND ----------

customers = spark.table(f"{BRONZE}.customers").drop("_source")
customers = customers.withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))

is_valid = F.col("customer_state").isin(valid_states)
reason = F.lit("unknown customer_state")
good, bad = split_good_bad(customers, is_valid, reason)

history = build_history(good, "customer_id",
                        ["customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"])
save_table(history, "customers")
save_table(bad, "quarantine_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 — Summary

# COMMAND ----------

display(spark.sql("""
    SELECT 'orders'         AS table_name, count(*) AS silver_rows FROM olist.silver.orders
    UNION ALL SELECT 'order_items',    count(*) FROM olist.silver.order_items
    UNION ALL SELECT 'order_payments', count(*) FROM olist.silver.order_payments
    UNION ALL SELECT 'order_reviews',  count(*) FROM olist.silver.order_reviews
    UNION ALL SELECT 'sellers (all versions)',   count(*) FROM olist.silver.sellers
    UNION ALL SELECT 'products (all versions)',  count(*) FROM olist.silver.products
    UNION ALL SELECT 'customers (all versions)', count(*) FROM olist.silver.customers
"""))