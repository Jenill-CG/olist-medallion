# Databricks notebook source
import urllib.request
try:
    ip = urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()
    print("Internet OK, serverless egress IP:", ip)
except Exception as e:
    print("No internet access:", e)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, order_status, reason
# MAGIC FROM olist.silver.quarantine_orders WHERE order_id = 'TEST_BAD_STATUS_001';
# MAGIC
# MAGIC %sql
# MAGIC SELECT count(*) FROM olist_src.app.orders;
# MAGIC
# MAGIC %sql
# MAGIC SELECT 1;