-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS olist
  MANAGED LOCATION 's3://olist-lakehouse-jt-0110-usw2/managed/';

CREATE SCHEMA IF NOT EXISTS olist.bronze;
CREATE SCHEMA IF NOT EXISTS olist.silver;
CREATE SCHEMA IF NOT EXISTS olist.gold;
CREATE SCHEMA IF NOT EXISTS olist.ops;

-- COMMAND ----------

SHOW SCHEMAS IN olist;

-- COMMAND ----------

SHOW STORAGE CREDENTIALS;
DESCRIBE STORAGE CREDENTIAL 'arn:aws:iam::082335234319:role/databricks-uc-7474652220432621-olist-lakehouse-jt-0110';

-- COMMAND ----------

DROP CATALOG IF EXISTS olist CASCADE;

CREATE CATALOG IF NOT EXISTS olist;
CREATE SCHEMA IF NOT EXISTS olist.bronze;
CREATE SCHEMA IF NOT EXISTS olist.silver;
CREATE SCHEMA IF NOT EXISTS olist.gold;
CREATE SCHEMA IF NOT EXISTS olist.ops;

-- COMMAND ----------

CREATE TABLE olist.ops.smoke_test AS SELECT 1 AS ok, current_timestamp() AS at;
SELECT * FROM olist.ops.smoke_test;
DROP TABLE olist.ops.smoke_test;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;
CREATE SCHEMA IF NOT EXISTS workspace.ops;

CREATE TABLE workspace.ops.smoke_test AS SELECT 1 AS ok, current_timestamp() AS at;
SELECT * FROM workspace.ops.smoke_test;
DROP TABLE workspace.ops.smoke_test;

-- COMMAND ----------

CREATE CONNECTION IF NOT EXISTS olist_rds TYPE postgresql
OPTIONS (
  host     secret('olist', 'rds_host'),
  port     '5432',
  user     secret('olist', 'rds_user'),
  password secret('olist', 'rds_password')
);

-- COMMAND ----------

CREATE FOREIGN CATALOG IF NOT EXISTS olist_src
USING CONNECTION olist_rds
OPTIONS (database 'olist');


-- COMMAND ----------

SELECT * FROM olist_src.ref.ref_order_status limit 10;

-- COMMAND ----------

SELECT 'sellers' t, count(*) n FROM olist_src.app.sellers
UNION ALL SELECT 'products', count(*) FROM olist_src.app.products
UNION ALL SELECT 'orders', count(*) FROM olist_src.app.orders
UNION ALL SELECT 'geolocation', count(*) FROM olist_src.ref.ref_geolocation;

-- COMMAND ----------

select * from olist_src.app.orders limit 10

-- COMMAND ----------

SHOW TABLES IN olist_src.app;
SHOW TABLES IN olist_src.ref;

-- COMMAND ----------

