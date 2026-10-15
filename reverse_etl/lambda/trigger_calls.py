"""
Lambda: trigger_calls  (Python 3.12, no extra libraries needed)

Trigger : S3 "ObjectCreated" on prefix  queue/delivery_risk/  with suffix  .csv
What    : reads the call list CSV written by Databricks (10_call_queue) and sends it to Bolna
Modes   : read from SSM  /olist/bolna/mode  on every run (change it in the AWS console, no redeploy)
            dry_run   -> creates a Bolna batch with ALL rows (fake numbers) and NEVER schedules it -> no call
            test_call -> sends ONLY the first row as one call to YOUR number (/olist/test_phone)
Output  : receipts/<sync_id>.json in the same bucket (read back by 10_call_queue)
          + one JSON log line + CloudWatch metrics (Embedded Metric Format, namespace Olist/ReverseETL)

SSM parameters:
  /olist/bolna/api_key   SecureString
  /olist/bolna/agent_id  String
  /olist/bolna/mode      String  (dry_run | test_call)
  /olist/test_phone      SecureString  (your own number, +91XXXXXXXXXX)

Lambda settings: timeout 60 s, memory 128 MB, asynchronous retries = 0 (so a failure never creates a second batch)
"""

import csv
import io
import json
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request

import boto3

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

BOLNA_URL = "https://api.bolna.ai"
REQUIRED_COLUMNS = ["contact_number", "seller_name", "order_id", "order_ref", "category",
                    "order_status", "promised_date", "days_left", "risk_band", "risk_percent"]
FAKE_NUMBER_PREFIX = "+9100000"     # Databricks only writes fake numbers; anything else is refused
MAX_ROWS = 20


# ---------- small helpers ----------

def get_param(name, secret=False):
    return ssm.get_parameter(Name=name, WithDecryption=secret)["Parameter"]["Value"]


def post(url, api_key, body, content_type):
    request = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": content_type,
    })
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def multipart_body(fields, file_field, file_name, file_bytes):
    """Build a multipart/form-data body by hand (the batch API needs a file upload, not JSON)."""
    boundary = uuid.uuid4().hex
    body = b""
    for key, value in fields.items():
        body += (f"--{boundary}\r\n"
                 f'Content-Disposition: form-data; name="{key}"\r\n\r\n'
                 f"{value}\r\n").encode("utf-8")
    body += (f"--{boundary}\r\n"
             f'Content-Disposition: form-data; name="{file_field}"; filename="{file_name}"\r\n'
             f"Content-Type: text/csv\r\n\r\n").encode("utf-8")
    body += file_bytes + b"\r\n"
    body += f"--{boundary}--\r\n".encode("utf-8")
    return body, f"multipart/form-data; boundary={boundary}"


def validate(rows, header):
    problems = []
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        problems.append(f"missing columns: {missing}")
    if len(rows) == 0:
        problems.append("no rows")
    if len(rows) > MAX_ROWS:
        problems.append(f"too many rows: {len(rows)} > {MAX_ROWS}")
    for r in rows:
        number = r.get("contact_number", "")
        if not number.startswith(FAKE_NUMBER_PREFIX):
            problems.append(f"refused: contact_number {number[:6]}... is not a fake POC number")
            break
    return problems


def emit_metrics(counts):
    """CloudWatch Embedded Metric Format: printing this JSON line creates the metrics."""
    print(json.dumps({
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "Olist/ReverseETL",
                "Dimensions": [["Service"]],
                "Metrics": [
                    {"Name": "RowsReceived", "Unit": "Count"},
                    {"Name": "BatchesCreated", "Unit": "Count"},
                    {"Name": "TestCalls", "Unit": "Count"},
                    {"Name": "ValidationErrors", "Unit": "Count"},
                    {"Name": "BolnaApiErrors", "Unit": "Count"},
                ],
            }],
        },
        "Service": "trigger_calls",
        "RowsReceived": counts["rows"],
        "BatchesCreated": counts["batches"],
        "TestCalls": counts["test_calls"],
        "ValidationErrors": counts["validation_errors"],
        "BolnaApiErrors": counts["api_errors"],
    }))


# ---------- main ----------

def lambda_handler(event, context):
    if "Records" in event:                       # direct S3 trigger
        s3_info = event["Records"][0]["s3"]
        bucket = s3_info["bucket"]["name"]
        key = urllib.parse.unquote_plus(s3_info["object"]["key"])
    else:                                        # EventBridge "Object Created" rule
        bucket = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"]
    if not (key.startswith("queue/delivery_risk/") and key.endswith(".csv")):
        print(json.dumps({"event": "trigger_calls", "key": key, "status": "ignored_not_a_call_list"}))
        return {"status": "ignored"}
    sync_id = key.rsplit("/", 1)[-1].replace("call_list_", "").replace(".csv", "")

    # Duplicate guard: S3 can deliver the same event twice. If a receipt already exists, skip.
    try:
        s3.head_object(Bucket=bucket, Key=f"receipts/{sync_id}.json")
        print(json.dumps({"event": "trigger_calls", "sync_id": sync_id, "status": "duplicate_skipped"}))
        return {"sync_id": sync_id, "status": "duplicate_skipped"}
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise

    counts ={"rows": 0, "batches": 0, "test_calls": 0, "validation_errors": 0, "api_errors": 0}
    receipt = {"sync_id": sync_id, "source_key": key, "mode": None, "reference_id": None,
               "status": None, "error": None, "orders": [], "processed_at": None}
    rows = []

    try:
        mode = get_param("/olist/bolna/mode")
        receipt["mode"] = mode

        file_bytes = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        reader = csv.DictReader(io.StringIO(file_bytes.decode("utf-8")))
        rows = list(reader)
        counts["rows"] = len(rows)

        problems = validate(rows, reader.fieldnames or [])
        if problems:
            counts["validation_errors"] = 1
            raise ValueError("; ".join(problems))

        api_key = get_param("/olist/bolna/api_key", secret=True)
        agent_id = get_param("/olist/bolna/agent_id")

        if mode == "dry_run":
            # Bolna only accepts real, valid numbers. The CSV from Databricks has fake numbers,
            # so here every number is replaced with YOUR test number (from SSM) before upload.
            # The batch is never scheduled (/schedule is never called), so no call is placed -
            # and even if it were scheduled by mistake, only your own phone could ring.
            test_phone = get_param("/olist/test_phone", secret=True)
            out = io.StringIO()
            writer = csv.DictWriter(out, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows([dict(r, contact_number=test_phone) for r in rows])
            upload_bytes = out.getvalue().encode("utf-8")

            body, content_type = multipart_body({"agent_id": agent_id}, "file",
                                                f"call_list_{sync_id}.csv", upload_bytes)
            response = post(f"{BOLNA_URL}/batches", api_key, body, content_type)
            receipt["reference_id"] = response.get("batch_id")
            receipt["orders"] = [{"order_id": r["order_id"], "status": "batch_created"} for r in rows]
            counts["batches"] = 1

        elif mode == "test_call":
            # One real call, only to your own number, with the first (riskiest) order's details.
            first = rows[0]
            payload = {
                "agent_id": agent_id,
                "recipient_phone_number": get_param("/olist/test_phone", secret=True),
                "user_data": {k: v for k, v in first.items() if k != "contact_number"},
            }
            response = post(f"{BOLNA_URL}/call", api_key,
                            json.dumps(payload).encode("utf-8"), "application/json")
            receipt["reference_id"] = response.get("execution_id")
            receipt["orders"] = ([{"order_id": first["order_id"], "status": "test_called"}] +
                                 [{"order_id": r["order_id"], "status": "skipped_test_mode"} for r in rows[1:]])
            counts["test_calls"] = 1

        else:
            raise ValueError(f"unknown mode '{mode}' (use dry_run or test_call)")

        receipt["status"] = "ok"

    except urllib.error.HTTPError as e:
        counts["api_errors"] = 1
        receipt["status"] = "failed"
        receipt["error"] = f"Bolna HTTP {e.code}: {e.read().decode('utf-8', 'ignore')[:300]}"
    except Exception as e:
        receipt["status"] = "failed"
        receipt["error"] = str(e)[:300]

    if receipt["status"] == "failed":
        receipt["orders"] = [{"order_id": r.get("order_id"), "status": "failed"} for r in rows]

    receipt["processed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    s3.put_object(Bucket=bucket, Key=f"receipts/{sync_id}.json",
                  Body=json.dumps(receipt).encode("utf-8"), ContentType="application/json")

    # One readable log line per run (easy to query in CloudWatch Logs Insights)
    print(json.dumps({"event": "trigger_calls", "sync_id": sync_id, "mode": receipt["mode"],
                      "rows": counts["rows"], "status": receipt["status"],
                      "reference_id": receipt["reference_id"], "error": receipt["error"]}))
    emit_metrics(counts)

    if receipt["status"] == "failed":
        raise RuntimeError(receipt["error"])      # marks the run as an error -> Lambda Errors alarm
    return {"sync_id": sync_id, "status": receipt["status"], "reference_id": receipt["reference_id"]}
