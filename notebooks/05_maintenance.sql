-- Databricks notebook source
-- 05_maintenance: daily Delta table maintenance (runs once a day, after the last pipeline run)
--   OPTIMIZE = compact many small files into fewer large ones  -> faster reads
--   VACUUM   = delete old, unused files older than 7 days       -> less storage
-- Default VACUUM retention (7 days) is kept on purpose: it protects time travel.

-- COMMAND ----------

-- Bronze: appended every hour -> collects many small files
OPTIMIZE olist.bronze.orders;
OPTIMIZE olist.bronze.order_items;
OPTIMIZE olist.bronze.order_payments;
OPTIMIZE olist.bronze.order_reviews;
OPTIMIZE olist.bronze.customers;
OPTIMIZE olist.bronze.sellers;
OPTIMIZE olist.bronze.products;

-- COMMAND ----------

-- Ops tables: appended every run
OPTIMIZE olist.ops.run_log;
OPTIMIZE olist.ops.check_results;

-- COMMAND ----------

-- Remove old unused files (older than 7 days) from bronze
VACUUM olist.bronze.orders;
VACUUM olist.bronze.order_items;
VACUUM olist.bronze.order_payments;
VACUUM olist.bronze.order_reviews;
VACUUM olist.bronze.customers;
VACUUM olist.bronze.sellers;
VACUUM olist.bronze.products;
VACUUM olist.bronze.ref_order_status;
VACUUM olist.bronze.ref_payment_type;
VACUUM olist.bronze.ref_state_region;
VACUUM olist.bronze.ref_product_category;
VACUUM olist.bronze.ref_geolocation;

-- COMMAND ----------

-- Silver and gold are overwritten every hour -> old versions pile up; clean them too
VACUUM olist.silver.orders;
VACUUM olist.silver.order_items;
VACUUM olist.silver.order_payments;
VACUUM olist.silver.order_reviews;
VACUUM olist.silver.customers;
VACUUM olist.silver.sellers;
VACUUM olist.silver.products;
VACUUM olist.gold.fact_sales;
VACUUM olist.gold.dim_customer;
VACUUM olist.gold.dim_seller;
VACUUM olist.gold.dim_product;
VACUUM olist.gold.dim_date;
VACUUM olist.gold.agg_daily_sales;
VACUUM olist.gold.agg_category_sales;
VACUUM olist.gold.agg_state_sales;
VACUUM olist.gold.ml_order_features;
VACUUM olist.gold.pred_late_delivery;

-- COMMAND ----------

-- Quick proof: file counts after maintenance
DESCRIBE DETAIL olist.bronze.orders;
