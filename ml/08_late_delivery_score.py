# Databricks notebook source
# MAGIC %md
# MAGIC # 08_late_delivery_score - score every OPEN order with the @champion model
# MAGIC
# MAGIC Runs DAILY at 10:40 IST (job `olist-ml-daily`) after `06_ml_features`.
# MAGIC Output: `olist.gold.pred_late_delivery` (one row per open order with a late-delivery risk between 0 and 1).
# MAGIC All open orders are scored; `is_new_yesterday = 1` marks orders created in RDS yesterday (IST).

# COMMAND ----------

import mlflow
import pandas as pd
from mlflow import MlflowClient
from pyspark.sql import functions as F

FEATURES_TABLE = "olist.gold.ml_order_features"
MODEL_NAME = "olist.gold.late_delivery_model"
OUTPUT_TABLE = "olist.gold.pred_late_delivery"

NUMERIC = ["est_days", "n_items", "n_sellers", "total_price", "total_freight", "freight_ratio",
           "total_weight_g", "max_volume_cm3", "distance_km", "same_state",
           "purchase_month", "purchase_dow", "purchase_hour"]
CATEGORICAL = ["customer_region", "seller_region", "customer_state", "seller_state", "main_category"]

mlflow.set_registry_uri("databricks-uc")

def prepare(frame):
    """Must match the training notebook exactly."""
    X = frame[NUMERIC + CATEGORICAL].copy()
    for c in CATEGORICAL:
        X[c] = X[c].fillna("Unknown").astype(str)
    for c in NUMERIC:
        X[c] = X[c].astype(float)
    return X

# COMMAND ----------

model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@champion")
model_version = MlflowClient().get_model_version_by_alias(MODEL_NAME, "champion").version

open_orders = (spark.table(FEATURES_TABLE)
                    .filter("is_open = 1")
                    .select("order_id", "order_status", "purchased_at", "estimated_delivery_at",
                            "first_seen_at", "is_new_yesterday", *NUMERIC, *CATEGORICAL)
                    .toPandas())
print("open orders to score:", len(open_orders),
      " of which created yesterday:", int(open_orders["is_new_yesterday"].sum()))

# COMMAND ----------

if len(open_orders) > 0:
    risk = model.predict_proba(prepare(open_orders))[:, 1]
    out = open_orders[["order_id", "order_status", "purchased_at", "estimated_delivery_at",
                       "first_seen_at", "is_new_yesterday",
                       "customer_state", "seller_state", "main_category", "distance_km", "est_days"]].copy()
    out["late_risk"] = risk.round(4)
    out["risk_band"] = pd.cut(out["late_risk"], bins=[-0.01, 0.4, 0.7, 1.0], labels=["Low", "Medium", "High"]).astype(str)
    out["model_version"] = str(model_version)

    (spark.createDataFrame(out)
          .withColumn("scored_at", F.current_timestamp())
          .write.mode("overwrite").option("overwriteSchema", "true")
          .saveAsTable(OUTPUT_TABLE))

    display(spark.sql(f"""
        SELECT risk_band,
               count(*)              AS open_orders,
               sum(is_new_yesterday) AS from_yesterday,
               round(avg(late_risk), 3) AS avg_risk
        FROM {OUTPUT_TABLE} GROUP BY risk_band ORDER BY avg_risk DESC"""))
else:
    print("No open orders right now - nothing to score.")