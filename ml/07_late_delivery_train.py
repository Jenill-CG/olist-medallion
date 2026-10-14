# Databricks notebook source
# MAGIC %md
# MAGIC # 07_late_delivery_train - train the late-delivery risk model
# MAGIC
# MAGIC **Question:** when an order is placed, how likely is it to arrive AFTER the promised date?
# MAGIC
# MAGIC | Step | What |
# MAGIC |---|---|
# MAGIC | 1 | Load delivered orders from `olist.gold.ml_order_features` (they have the answer `is_late`) |
# MAGIC | 2 | Split by TIME: oldest 80% to train, newest 20% to test (no peeking into the future) |
# MAGIC | 3 | Train a gradient boosting classifier |
# MAGIC | 4 | Measure: ROC AUC, PR AUC, and how many late orders the top 10% riskiest catch |
# MAGIC | 5 | Log everything to MLflow and register the model in Unity Catalog with alias `champion` |
# MAGIC
# MAGIC Run on demand (e.g. weekly). Scoring happens hourly in `08_late_delivery_score`.
# MAGIC
# MAGIC If an import fails, run once in a cell: `%pip install scikit-learn` then `dbutils.library.restartPython()`

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from mlflow import MlflowClient
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.inspection import permutation_importance

FEATURES_TABLE = "olist.gold.ml_order_features"
MODEL_NAME = "olist.gold.late_delivery_model"

NUMERIC = ["est_days", "n_items", "n_sellers", "total_price", "total_freight", "freight_ratio",
           "total_weight_g", "max_volume_cm3", "distance_km", "same_state",
           "purchase_month", "purchase_dow", "purchase_hour"]
CATEGORICAL = ["customer_region", "seller_region", "customer_state", "seller_state", "main_category"]

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load training data (delivered orders only)

# COMMAND ----------

df = (spark.table(FEATURES_TABLE)
           .filter("is_late IS NOT NULL")
           .select("order_id", "purchased_at", "is_late", *NUMERIC, *CATEGORICAL)
           .toPandas())

def prepare(frame):
    """Same cleaning for training and scoring: text -> 'Unknown' if missing, numbers -> float."""
    X = frame[NUMERIC + CATEGORICAL].copy()
    for c in CATEGORICAL:
        X[c] = X[c].fillna("Unknown").astype(str)
    for c in NUMERIC:
        X[c] = X[c].astype(float)          # NaN is fine: the model handles missing values
    return X

print("orders:", len(df), " late share:", round(df["is_late"].mean() * 100, 1), "%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Time-based split (train on the past, test on the "future")

# COMMAND ----------

df = df.sort_values("purchased_at").reset_index(drop=True)
cut = int(len(df) * 0.8)
train, test = df.iloc[:cut], df.iloc[cut:]

X_train, y_train = prepare(train), train["is_late"].astype(int)
X_test,  y_test  = prepare(test),  test["is_late"].astype(int)

print("train:", len(train), "orders up to", train["purchased_at"].max())
print("test :", len(test),  "orders from",  test["purchased_at"].min())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3-5 - Train, evaluate, log to MLflow, register

# COMMAND ----------

model = Pipeline([
    ("prep", ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=50), CATEGORICAL)],
        remainder="passthrough",          # numeric columns go through unchanged
        sparse_threshold=0)),
    ("clf", HistGradientBoostingClassifier(
        max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
        class_weight="balanced",          # late orders are rare (~8%), so give them more weight
        random_state=42)),
])

with mlflow.start_run(run_name="late_delivery_hgb") as run:
    model.fit(X_train, y_train)
    proba = model.predict_proba(X_test)[:, 1]

    # How well does the risk score rank late orders?
    roc_auc = roc_auc_score(y_test, proba)
    pr_auc = average_precision_score(y_test, proba)

    # Business view: if ops checks only the riskiest 10% of orders, how many late ones do they catch?
    top = proba >= np.quantile(proba, 0.90)
    precision_top10 = y_test[top].mean()
    recall_top10 = y_test[top].sum() / max(y_test.sum(), 1)
    base_rate = y_test.mean()
    lift_top10 = precision_top10 / base_rate if base_rate > 0 else 0

    mlflow.log_params({"model": "HistGradientBoostingClassifier", "max_iter": 300, "learning_rate": 0.05,
                       "train_rows": len(train), "test_rows": len(test), "split": "time-based 80/20"})
    mlflow.log_metrics({"roc_auc": roc_auc, "pr_auc": pr_auc, "base_late_rate": base_rate,
                        "precision_top10pct": precision_top10, "recall_top10pct": recall_top10,
                        "lift_top10pct": lift_top10})

    sample = X_train.head(100)
    signature = infer_signature(sample, model.predict_proba(sample)[:, 1])
    mlflow.sklearn.log_model(model, artifact_path="model", signature=signature,
                             input_example=X_train.head(5), registered_model_name=MODEL_NAME)

print(f"ROC AUC          : {roc_auc:.3f}   (0.5 = random, 1.0 = perfect)")
print(f"PR AUC           : {pr_auc:.3f}   (baseline = late share {base_rate:.3f})")
print(f"Top 10% riskiest : {precision_top10*100:.1f}% are late  ->  lift x{lift_top10:.1f}")
print(f"                   they contain {recall_top10*100:.1f}% of all late orders")

# COMMAND ----------

# Point the alias "champion" at the version we just registered (scoring always uses @champion)
client = MlflowClient()
latest = max(int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'"))
client.set_registered_model_alias(MODEL_NAME, "champion", latest)
print(f"{MODEL_NAME} version {latest} is now @champion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which features matter most? (permutation importance on the test set)

# COMMAND ----------

sample_idx = X_test.sample(min(5000, len(X_test)), random_state=42).index
imp = permutation_importance(model, X_test.loc[sample_idx], y_test.loc[sample_idx],
                             scoring="roc_auc", n_repeats=3, random_state=42)
importance = (pd.DataFrame({"feature": X_test.columns, "importance": imp.importances_mean})
                .sort_values("importance", ascending=False))
display(spark.createDataFrame(importance))