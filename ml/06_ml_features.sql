-- Databricks notebook source
-- 06_ml_features: one row per ORDER with everything known at purchase time
-- Used by: 07_late_delivery_train (delivered orders = training data)
--          08_late_delivery_score (open orders = orders to score)
-- Runs DAILY at 10:40 IST in job olist-ml-daily (after the 10:05 pipeline run).

-- COMMAND ----------

CREATE OR REPLACE TABLE olist.gold.ml_order_features
COMMENT 'Order-level features for the late-delivery model (only information known at purchase time)'
AS
WITH order_items AS (
  SELECT
    f.order_id,
    min(f.purchased_at)              AS purchased_at,
    min(f.estimated_delivery_at)     AS estimated_delivery_at,
    min(f.delivered_at)              AS delivered_at,
    min(f.order_status)              AS order_status,
    min(f.customer_key)              AS customer_key,
    count(*)                         AS n_items,
    count(DISTINCT f.seller_key)     AS n_sellers,
    sum(f.price)                     AS total_price,
    sum(f.freight_value)             AS total_freight,
    sum(p.product_weight_g)          AS total_weight_g,
    max(p.product_length_cm * p.product_height_cm * p.product_width_cm) AS max_volume_cm3,
    -- for multi-item orders, take the details of the most expensive item
    max_by(p.category_en, f.price)   AS main_category,
    max_by(s.state_code,  f.price)   AS seller_state,
    max_by(s.region,      f.price)   AS seller_region,
    max_by(s.lat,         f.price)   AS seller_lat,
    max_by(s.lng,         f.price)   AS seller_lng
  FROM olist.gold.fact_sales f
  LEFT JOIN olist.gold.dim_product p ON f.product_key = p.product_key
  LEFT JOIN olist.gold.dim_seller  s ON f.seller_key  = s.seller_key
  GROUP BY f.order_id
),
-- when the order first appeared in RDS (system time, 2026).
-- updated_at is set by the RDS trigger, so min(updated_at) = order creation time in RDS
first_seen AS (
  SELECT order_id, min(updated_at) AS first_seen_at
  FROM olist.bronze.orders
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.purchased_at,
  o.estimated_delivery_at,
  o.delivered_at,
  o.order_status,
  -- features (all known when the order is placed)
  datediff(o.estimated_delivery_at, o.purchased_at)        AS est_days,
  o.n_items,
  o.n_sellers,
  o.total_price,
  o.total_freight,
  o.total_freight / nullif(o.total_price, 0)                AS freight_ratio,
  o.total_weight_g,
  o.max_volume_cm3,
  o.main_category,
  o.seller_state,
  o.seller_region,
  c.state_code                                              AS customer_state,
  c.region                                                  AS customer_region,
  CASE WHEN c.state_code = o.seller_state THEN 1 ELSE 0 END AS same_state,
  -- straight-line distance seller -> customer in km (haversine formula)
  6371 * 2 * asin(sqrt(
      pow(sin(radians(c.lat - o.seller_lat) / 2), 2)
    + cos(radians(o.seller_lat)) * cos(radians(c.lat)) * pow(sin(radians(c.lng - o.seller_lng) / 2), 2)
  ))                                                        AS distance_km,
  month(o.purchased_at)                                     AS purchase_month,
  dayofweek(o.purchased_at)                                 AS purchase_dow,
  hour(o.purchased_at)                                      AS purchase_hour,
  -- label: only known AFTER delivery (never used as a feature)
  CASE WHEN o.delivered_at IS NOT NULL AND o.estimated_delivery_at IS NOT NULL
       THEN CAST(o.delivered_at > o.estimated_delivery_at AS INT) END   AS is_late,
  -- open = not finished yet -> these get scored
  CASE WHEN o.order_status IN ('created','approved','invoiced','processing','shipped')
       THEN 1 ELSE 0 END                                    AS is_open,
  -- daily 10 AM run: flag orders created in RDS "yesterday" (IST calendar day)
  fs.first_seen_at,
  CASE WHEN to_date(from_utc_timestamp(fs.first_seen_at, 'Asia/Kolkata'))
            = date_sub(to_date(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')), 1)
       THEN 1 ELSE 0 END                                    AS is_new_yesterday,
  current_timestamp()                                       AS _features_built_at
FROM order_items o
LEFT JOIN olist.gold.dim_customer c ON o.customer_key = c.customer_key
LEFT JOIN first_seen fs              ON o.order_id     = fs.order_id;

-- COMMAND ----------

SELECT
  count(*)                                  AS orders,
  sum(CASE WHEN is_late IS NOT NULL THEN 1 ELSE 0 END) AS training_rows,
  round(avg(is_late) * 100, 1)              AS late_pct,
  sum(is_open)                              AS open_orders_to_score,
  sum(is_new_yesterday)                     AS orders_created_yesterday,
  sum(is_open * is_new_yesterday)           AS open_orders_from_yesterday
FROM olist.gold.ml_order_features;