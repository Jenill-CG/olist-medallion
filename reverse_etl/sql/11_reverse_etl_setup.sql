-- Databricks notebook source
-- 11_reverse_etl_setup: one-time setup for the reverse ETL to Bolna (run once, on Friday)
--
-- Before running:
--   1. The S3 bucket exists (e.g. olist-calls-jenil)
--   2. A Unity Catalog storage credential + external location covering that bucket exist
--      (Catalog -> External data -> Credentials / External locations)
--   3. Replace <your-bucket> below with the real bucket name

-- COMMAND ----------

-- A Volume = a folder in S3 that notebooks can read/write with plain Python (open, os.listdir)
--   /Volumes/olist/ops/calls/queue/delivery_risk/  <- 10_call_queue writes CSV files here
--   /Volumes/olist/ops/calls/receipts/             <- Lambda writes a receipt per CSV here
CREATE EXTERNAL VOLUME IF NOT EXISTS olist.ops.calls
LOCATION 's3://olist-calls-jenil-2026'
COMMENT 'Reverse ETL handoff folder: call lists to Bolna (via Lambda) and Lambda receipts';

-- COMMAND ----------

-- One row per order sent to Bolna. Used for:
--   - no duplicates: an order already in this table is never sent again
--   - tracking: sent -> batch_created / test_called / skipped_test_mode / failed
CREATE TABLE IF NOT EXISTS olist.ops.reverse_etl_log (
  sync_id       STRING    COMMENT 'one per 10_call_queue run, e.g. sync_20261016_104512',
  destination   STRING    COMMENT 'bolna',
  order_id      STRING,
  seller_id     STRING,
  late_risk     DOUBLE,
  risk_band     STRING,
  file_path     STRING    COMMENT 'CSV file written to the Volume',
  reference_id  STRING    COMMENT 'Bolna batch_id (dry_run) or execution_id (test_call), from the Lambda receipt',
  status        STRING    COMMENT 'sent / batch_created / test_called / skipped_test_mode / failed',
  synced_at     TIMESTAMP COMMENT 'when the row was sent',
  updated_at    TIMESTAMP COMMENT 'last status change'
)
COMMENT 'Reverse ETL log: gold.pred_late_delivery -> S3 -> Lambda -> Bolna';

-- COMMAND ----------

-- Quick check
SHOW VOLUMES IN olist.ops;
DESCRIBE TABLE olist.ops.reverse_etl_log;
