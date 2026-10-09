# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 03_gold — silver ➜ gold (star schema for reports)
# MAGIC
# MAGIC Same style as 02_silver: every run **rebuilds** each gold table from silver. Running it twice gives the same result.
# MAGIC
# MAGIC ```
# MAGIC                 dim_date
# MAGIC                    │
# MAGIC  dim_customer ── fact_sales ── dim_product
# MAGIC                    │
# MAGIC                dim_seller
# MAGIC ```
# MAGIC
# MAGIC | Table | One row per | Built from |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | calendar day 2016–2019 | generated (no source) |
# MAGIC | `dim_customer` | customer | silver.customers (current) + region + geolocation |
# MAGIC | `dim_seller` | seller **version** (SCD2) | silver.sellers (all versions) + region + geolocation |
# MAGIC | `dim_product` | product **version** (SCD2) | silver.products (all versions) + English category |
# MAGIC | `fact_sales` | order item | silver.order_items + orders + reviews, linked to the dims |
# MAGIC | `agg_*` | day / category / state | fact_sales + dims, ready for the dashboard |
# MAGIC
# MAGIC Every dimension has a **-1 "Unknown" row**, so a sale is never dropped because a lookup is missing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Setup and helpers

# COMMAND ----------

from pyspark.sql import functions as F

SILVER = "olist.silver"
GOLD = "olist.gold"


def save_table(df, table_name):
    """Write a DataFrame to olist.gold.<table_name>, replacing whatever was there."""
    full_name = f"{GOLD}.{table_name}"
    df = df.withColumn("_gold_loaded_at", F.current_timestamp())
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    print(f"saved {full_name:32} rows = {spark.table(full_name).count()}")


def add_unknown_row(df, key_column):
    """
    Add one extra row: key = -1, text columns = 'Unknown', everything else empty (NULL).
    Facts that can't find a match point to this row instead of disappearing.
    """
    unknown = df.limit(1)                       # copy the column layout from one real row
    for col_name, col_type in df.dtypes:
        if col_name == key_column:
            unknown = unknown.withColumn(col_name, F.lit(-1).cast(col_type))
        elif col_type == "string":
            unknown = unknown.withColumn(col_name, F.lit("Unknown"))
        else:
            unknown = unknown.withColumn(col_name, F.lit(None).cast(col_type))
    return df.unionByName(unknown)


# lookups used by several dimensions
regions = spark.table(f"{SILVER}.ref_state_region").select("state_code", "state_name", "region")
geo = spark.table(f"{SILVER}.ref_geolocation").select("zip_prefix", "lat", "lng")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — dim_date (generated calendar)
# MAGIC
# MAGIC No source table needed: we generate one row per day. `date_key` is the date as a number, e.g. 2017-03-15 → 20170315.

# COMMAND ----------

dates = spark.sql("""
    SELECT explode(sequence(DATE'2016-01-01', DATE'2019-12-31', INTERVAL 1 DAY)) AS date
""")

dim_date = dates.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
dim_date = dim_date.withColumn("year", F.year("date"))
dim_date = dim_date.withColumn("quarter", F.quarter("date"))
dim_date = dim_date.withColumn("month", F.month("date"))
dim_date = dim_date.withColumn("month_name", F.date_format("date", "MMMM"))       # March
dim_date = dim_date.withColumn("year_month", F.date_format("date", "yyyy-MM"))    # 2017-03
dim_date = dim_date.withColumn("day", F.dayofmonth("date"))
dim_date = dim_date.withColumn("day_name", F.date_format("date", "EEEE"))         # Wednesday
dim_date = dim_date.withColumn("is_weekend", F.dayofweek("date").isin(1, 7))      # 1 = Sunday, 7 = Saturday

dim_date = add_unknown_row(dim_date, "date_key")
save_table(dim_date, "dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — dim_customer
# MAGIC
# MAGIC Customers never change in our data, so we take the current version only.
# MAGIC `customer_key` is a number made from `customer_id` with `xxhash64` (same id → same number on every run).

# COMMAND ----------

customers = spark.table(f"{SILVER}.customers").filter(F.col("is_current") == True)

dim_customer = customers.select(
    F.xxhash64("customer_id").alias("customer_key"),
    "customer_id",
    "customer_unique_id",
    F.col("customer_city").alias("city"),
    F.col("customer_state").alias("state_code"),
    F.col("customer_zip_code_prefix").alias("zip_prefix"),
)
dim_customer = dim_customer.join(regions, on="state_code", how="left")   # adds state_name, region
dim_customer = dim_customer.join(geo, on="zip_prefix", how="left")       # adds lat, lng

dim_customer = add_unknown_row(dim_customer, "customer_key")
save_table(dim_customer, "dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — dim_seller (all versions, SCD Type 2)
# MAGIC
# MAGIC **Two kinds of time in this project:**
# MAGIC
# MAGIC | Time | Example | Meaning |
# MAGIC |---|---|---|
# MAGIC | Business time | `order_purchase_timestamp` = 2017-03-15 | the original Olist date — used for calendar reporting |
# MAGIC | System time | `updated_at` = 2026-10-06 12:15 | when the row changed in our live shop (RDS) |
# MAGIC
# MAGIC Seller versions change in **system time**, so a sale must be matched to a seller version using the sale's
# MAGIC system time too (the order item's `updated_at`, i.e. when the sale was recorded in the shop).
# MAGIC
# MAGIC `effective_from` / `effective_to` are the ranges used for that match:
# MAGIC - version 1 is valid "from the beginning" (1900-01-01), so nothing falls before it
# MAGIC - the current version is valid "until forever" (9999-12-31)

# COMMAND ----------

sellers = spark.table(f"{SILVER}.sellers")

dim_seller = sellers.select(
    F.xxhash64("seller_id", "valid_from").alias("seller_key"),   # one key per VERSION
    "seller_id",
    "version",
    F.col("seller_city").alias("city"),
    F.col("seller_state").alias("state_code"),
    F.col("seller_zip_code_prefix").alias("zip_prefix"),
    "valid_from",
    "valid_to",
    "is_current",
)
dim_seller = dim_seller.withColumn(
    "effective_from",
    F.when(F.col("version") == 1, F.to_timestamp(F.lit("1900-01-01"))).otherwise(F.col("valid_from")),
)
dim_seller = dim_seller.withColumn(
    "effective_to",
    F.coalesce(F.col("valid_to"), F.to_timestamp(F.lit("9999-12-31"))),
)
dim_seller = dim_seller.join(regions, on="state_code", how="left")
dim_seller = dim_seller.join(geo, on="zip_prefix", how="left")

dim_seller = add_unknown_row(dim_seller, "seller_key")
save_table(dim_seller, "dim_seller")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — dim_product (all versions, SCD Type 2)

# COMMAND ----------

products = spark.table(f"{SILVER}.products")
categories = spark.table(f"{SILVER}.ref_product_category").select("category_pt", "category_en")

dim_product = products.select(
    F.xxhash64("product_id", "valid_from").alias("product_key"),
    "product_id",
    "version",
    F.col("product_category_name").alias("category_pt"),
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "valid_from",
    "valid_to",
    "is_current",
)
dim_product = dim_product.join(categories, on="category_pt", how="left")
dim_product = dim_product.withColumn("category_en", F.coalesce(F.col("category_en"), F.lit("unknown")))
dim_product = dim_product.withColumn(
    "effective_from",
    F.when(F.col("version") == 1, F.to_timestamp(F.lit("1900-01-01"))).otherwise(F.col("valid_from")),
)
dim_product = dim_product.withColumn(
    "effective_to",
    F.coalesce(F.col("valid_to"), F.to_timestamp(F.lit("9999-12-31"))),
)

dim_product = add_unknown_row(dim_product, "product_key")
save_table(dim_product, "dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — fact_sales (one row per order item)
# MAGIC
# MAGIC 1. start from order items (the sale lines)
# MAGIC 2. add order details (status, dates) and the average review score of the order
# MAGIC 3. look up each dimension key — if no match, use -1 (Unknown)
# MAGIC 4. calculate measures: item total, delivery days, late flag

# COMMAND ----------

items = spark.table(f"{SILVER}.order_items").alias("i")
orders = spark.table(f"{SILVER}.orders").alias("o")

reviews = (spark.table(f"{SILVER}.order_reviews")
           .groupBy("order_id")
           .agg(F.avg("review_score").alias("review_score"))     # an order can have more than one review
           .alias("r"))

cust = spark.table(f"{GOLD}.dim_customer").select("customer_key", "customer_id").alias("c")
sell = spark.table(f"{GOLD}.dim_seller").select("seller_key", "seller_id", "effective_from", "effective_to").alias("s")
prod = spark.table(f"{GOLD}.dim_product").select("product_key", "product_id", "effective_from", "effective_to").alias("p")

# 1 + 2: items + order details + review score
fact = items.join(orders, F.col("i.order_id") == F.col("o.order_id"), "left")
fact = fact.join(reviews, F.col("i.order_id") == F.col("r.order_id"), "left")

# 3a: customer (simple match on id)
fact = fact.join(cust, F.col("o.customer_id") == F.col("c.customer_id"), "left")

# 3b: seller VERSION valid when the sale was recorded (system time = item's updated_at)
fact = fact.join(
    sell,
    (F.col("i.seller_id") == F.col("s.seller_id"))
    & (F.col("i.updated_at") >= F.col("s.effective_from"))
    & (F.col("i.updated_at") < F.col("s.effective_to")),
    "left",
)

# 3c: product VERSION valid when the sale was recorded
fact = fact.join(
    prod,
    (F.col("i.product_id") == F.col("p.product_id"))
    & (F.col("i.updated_at") >= F.col("p.effective_from"))
    & (F.col("i.updated_at") < F.col("p.effective_to")),
    "left",
)

# 4: pick the final columns and calculate the measures
fact_sales = fact.select(
    F.col("i.order_id").alias("order_id"),
    F.col("i.order_item_id").alias("order_item_id"),
    # keys (-1 = Unknown when no match was found)
    F.coalesce(F.date_format("o.order_purchase_timestamp", "yyyyMMdd").cast("int"), F.lit(-1)).alias("date_key"),
    F.coalesce(F.col("c.customer_key"), F.lit(-1)).alias("customer_key"),
    F.coalesce(F.col("s.seller_key"), F.lit(-1)).alias("seller_key"),
    F.coalesce(F.col("p.product_key"), F.lit(-1)).alias("product_key"),
    # order details
    F.coalesce(F.col("o.order_status"), F.lit("Unknown")).alias("order_status"),
    F.col("o.order_purchase_timestamp").alias("purchased_at"),
    F.col("o.order_delivered_customer_date").alias("delivered_at"),
    F.col("o.order_estimated_delivery_date").alias("estimated_delivery_at"),
    F.col("i.updated_at").alias("sale_recorded_at"),          # system time, used for the SCD2 match
    # measures
    F.col("i.price").alias("price"),
    F.col("i.freight_value").alias("freight_value"),
    (F.col("i.price") + F.col("i.freight_value")).alias("item_total"),
    F.datediff("o.order_delivered_customer_date", "o.order_purchase_timestamp").alias("delivery_days"),
    (F.col("o.order_delivered_customer_date") > F.col("o.order_estimated_delivery_date")).alias("is_late"),
    F.col("r.review_score").alias("review_score"),
)

save_table(fact_sales, "fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Summary tables for the dashboard
# MAGIC
# MAGIC Plain SQL: join the fact to a dimension and group. The dashboard reads these small tables, so it stays fast.

# COMMAND ----------

agg_daily_sales = spark.sql("""
    SELECT d.date,
           d.year_month,
           count(DISTINCT f.order_id) AS orders,
           count(*)                   AS items,
           sum(f.price)               AS revenue,
           sum(f.freight_value)       AS freight,
           avg(f.review_score)        AS avg_review_score
    FROM olist.gold.fact_sales f
    JOIN olist.gold.dim_date d ON f.date_key = d.date_key
    WHERE f.date_key <> -1
    GROUP BY d.date, d.year_month
""")
save_table(agg_daily_sales, "agg_daily_sales")

agg_category_sales = spark.sql("""
    SELECT p.category_en               AS category,
           count(DISTINCT f.order_id)  AS orders,
           count(*)                    AS items,
           sum(f.price)                AS revenue,
           avg(f.review_score)         AS avg_review_score
    FROM olist.gold.fact_sales f
    JOIN olist.gold.dim_product p ON f.product_key = p.product_key
    GROUP BY p.category_en
""")
save_table(agg_category_sales, "agg_category_sales")

agg_state_sales = spark.sql("""
    SELECT c.state_code,
           c.state_name,
           c.region,
           count(DISTINCT f.order_id)                           AS orders,
           sum(f.price)                                         AS revenue,
           avg(f.delivery_days)                                 AS avg_delivery_days,
           avg(CASE WHEN f.is_late THEN 1.0 ELSE 0.0 END)
               FILTER (WHERE f.delivered_at IS NOT NULL)        AS late_share
    FROM olist.gold.fact_sales f
    JOIN olist.gold.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.state_code, c.state_name, c.region
""")
save_table(agg_state_sales, "agg_state_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Pipeline freshness
# MAGIC
# MAGIC One row: when each layer last got data, and how many minutes gold is behind the newest change we have seen.

# COMMAND ----------

freshness = spark.sql("""
    SELECT (SELECT max(updated_at)        FROM olist.bronze.orders)     AS latest_source_change,
           (SELECT max(_ingested_at)      FROM olist.bronze.orders)     AS bronze_loaded_at,
           (SELECT max(_silver_loaded_at) FROM olist.silver.orders)     AS silver_loaded_at,
           current_timestamp()                                          AS gold_loaded_at
""")
freshness = freshness.withColumn(
    "lag_minutes",
    ((F.unix_timestamp("gold_loaded_at") - F.unix_timestamp("latest_source_change")) / 60).cast("int"),
)
freshness.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist.ops.pipeline_freshness")
display(freshness)