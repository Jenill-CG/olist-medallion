# Demo script (6-7 minutes)

**Before the demo (30-40 min earlier):**
1. RDS -> Start -> wait until **Available**.
2. Unpause `olist-pipeline` -> **Run now** -> wait until green (freshness fresh, 16 checks PASS).
3. Confirm `/olist/bolna/mode` = `dry_run`.
4. Open these tabs in order: Databricks dashboard, Catalog, Jobs, CloudWatch `olist-poc`, Bolna, GitHub repo.
5. Hide/close anything with account IDs, phone numbers or keys.

---

## 1. The problem (30 s)
"Late deliveries hurt marketplaces. Ops teams usually find out after the customer complains.
I built a small lakehouse that predicts which open orders are likely to be late and automatically
hands them to an AI voice agent that calls the seller."

## 2. Source and ingestion (1 min) - tab: Catalog / Jobs
- RDS PostgreSQL in Mumbai holds the shop's tables; a `pg_cron` job replays real Olist orders so the data is "live".
- Databricks reads it through **Lakehouse Federation** - no credentials in notebooks.
- Show the `olist-pipeline` graph: bronze (incremental, watermark) -> silver (cleaning, SCD2, quarantine) -> gold (star schema) -> 16 data-quality checks. Runs hourly.

## 3. Dashboard (1 min) - tab: dashboard
- Sales KPIs and freshness (minutes since last load).
- Scroll to the **late-delivery risk table**: risk %, days to promise, Overdue / Due soon.

## 4. The model (1 min) - tab: model / MLflow
- Gradient boosting on purchase-time features, time-based split, registered as `@champion` in Unity Catalog.
- "Calling only the riskiest 10% of orders reaches **27% of all late deliveries** - 2.7x better than random."
- Scored daily at 10:40 by `olist-ml-daily`.

## 5. Reverse ETL to a voice agent (1.5 min) - tabs: S3 / Bolna
- Same job, last task: `call_queue` writes yesterday's riskiest orders to S3.
- S3 -> EventBridge -> Lambda -> Bolna. Secrets in SSM; fake numbers in the data; dry run creates an unscheduled batch.
- Show the **Bolna batch** and play 20-30 s of the **test call** recording / show the transcript:
  "This order from the lakehouse became this call."

## 6. Monitoring and CI/CD (1 min) - tabs: CloudWatch / GitHub
- CloudWatch dashboard: runs, errors, RDS health; alarms email me (show the alarm email).
- GitHub Actions deploys the Lambda with OIDC - no AWS keys stored anywhere.

## 7. Wrap-up (30 s)
- "Built in 7 days on a trial budget. Next steps: store call outcomes back in the lakehouse
  (Bolna webhook), add seller-history features, and calibrate the risk scores."
- Repo + screenshots: GitHub link.

---

**Backup if something is down:** use the screenshots in `docs/screenshots/` in the same order.
