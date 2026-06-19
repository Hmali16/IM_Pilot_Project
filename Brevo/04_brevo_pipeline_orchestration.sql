-- Brevo pipeline: Generate CSVs to stage → COPY INTO Bronze → Remove files → Streams → Silver → Gold
-- Co-authored with CoCo
-- =============================================================================
-- IM PILOT PROJECT | BREVO SOURCE | PIPELINE ORCHESTRATION
-- Complete flow:
--   1. GENERATE_INCREMENTAL_DATA proc creates CSVs and PUTs them to stage
--   2. TASK_LOAD_STAGE_TO_BRONZE does COPY INTO from stage to Bronze tables
--   3. TASK_CLEANUP_STAGE removes files from stage
--   4. Streams detect new rows in Bronze
--   5. Silver MERGE tasks process incremental data
--   6. Gold MERGE tasks build dimensions and facts
-- =============================================================================

-- =============================================================================
-- TASK DAG STRUCTURE:
--
--   TASK_GENERATE_TEST_DATA (every 30 min - generates CSVs to stage)
--       │
--   TASK_BREVO_ROOT (every 5 min)
--       └── TASK_LOAD_STAGE_TO_BRONZE (COPY INTO from stage)
--               └── TASK_CLEANUP_STAGE (REMOVE files from stage)
--                       └── TASK_SILVER_ROOT (checks streams)
--                               ├── TASK_MERGE_SLV_CONTACT
--                               ├── TASK_MERGE_SLV_CONTACT_LIST
--                               ├── TASK_MERGE_SLV_EVENT
--                               ├── TASK_MERGE_SLV_AGG_REPORT
--                               ├── TASK_MERGE_SLV_SMTP_EMAIL
--                               └── TASK_MERGE_SLV_SMTP_EVENT
--                                       │
--                               TASK_GOLD_ROOT
--                                   ├── TASK_MERGE_DIM_CONTACT
--                                   ├── TASK_MERGE_DIM_POLICY
--                                   ├── TASK_MERGE_DIM_PLAN
--                                   └── TASK_MERGE_FACT_EVENT
--
-- =============================================================================

-- =============================================================================
-- STEP 0: Ensure schemas exist
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS BREVO.BRONZE;
CREATE SCHEMA IF NOT EXISTS BREVO.SILVER;
CREATE SCHEMA IF NOT EXISTS BREVO.GOLD;

-- =============================================================================
-- STEP 1A: PROCEDURE - Generate CSVs and PUT to stage
-- Creates CSV files in /tmp inside Snowflake compute, then PUTs them to
-- @BREVO.BRONZE.STG_BREVO using session.file.put()
-- =============================================================================

CREATE OR REPLACE PROCEDURE BREVO.BRONZE.GENERATE_INCREMENTAL_DATA(NUM_RECORDS INT)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_data'
AS
$$
import csv
import random
import hashlib
import json
import uuid
import os
from datetime import datetime, timedelta

def generate_data(session, num_records):
    FIRST_NAMES = ["Jaimin", "Aisha", "Mohammed", "Fatima", "Ahmed", "Sara", "Omar", "Layla"]
    LAST_NAMES = ["Williams", "Al Suwaidi", "Khan", "Patel", "Singh", "Ahmed", "Johnson", "Smith"]
    VEHICLE_MAKES = ["Toyota", "Honda", "Ford", "BMW", "Tesla", "Hyundai"]
    EVENT_NAMES = ["policy_created", "policy_renewed", "payment_received", "claim_submitted"]
    POLICY_CODES = ["AUTO", "HOME", "LIFE", "HEALTH", "TRAVEL"]
    COVERAGE_CODES = ["BASIC", "STANDARD", "PREMIUM", "COMPREHENSIVE"]
    PAYMENT_GATEWAYS = ["Stripe", "PayPal", "RazorPay", "BankTransfer"]
    SMTP_EVENT_TYPES = ["delivered", "opened", "clicked", "bounced", "soft_bounced"]
    LIST_NAMES = ["Newsletter", "Policy Holders", "Renewal Due", "New Customers", "VIP Clients"]

    BATCH_ID = f"BREVO_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8].upper()}"
    OUTPUT_DIR = "/tmp/brevo_incremental"
    STAGE = "@BREVO.BRONZE.STG_BREVO"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    def now_ts():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def rand_ts(days_back=30):
        dt = datetime.now() - timedelta(days=random.randint(0, days_back), hours=random.randint(0, 23))
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def rand_future_date(days_ahead=365):
        return (datetime.now() + timedelta(days=random.randint(30, days_ahead))).strftime("%Y-%m-%d")

    def rand_email(first, last):
        domains = ["gmail.com", "yahoo.com", "outlook.com"]
        return f"{first.lower()}.{last.lower().replace(' ', '')}{random.randint(1,99)}@{random.choice(domains)}"

    def record_hash(data_str):
        return hashlib.sha256(data_str.encode()).hexdigest()

    def type1_hash(fields):
        return hashlib.sha256("|".join(str(f) for f in fields).encode()).hexdigest()[:32]

    def write_csv(filename, rows):
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys(), quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(rows)
        return filepath

    now = now_ts()
    files_uploaded = []

    # --- BREVO_CONTACT ---
    rows = []
    for i in range(num_records):
        cid = 3000001 + random.randint(0, 99999)
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        email = rand_email(first, last)
        list_ids = json.dumps(random.sample(range(1, 50), random.randint(1, 3)))
        dob = (datetime.now() - timedelta(days=random.randint(7000, 20000))).strftime("%Y-%m-%d")
        rows.append({
            "Id": cid, "Email": email,
            "Email Blacklisted": random.choice(["TRUE", "FALSE"]),
            "Sms Blacklisted": "FALSE",
            "Created At Date": rand_ts(60), "Modified At Date": now,
            "List Ids": list_ids, "List Unsubscribed": "",
            "Attributes": json.dumps({"FIRSTNAME": first, "LASTNAME": last}),
            "First Name": first, "Last Name": last, "Dob": dob,
            "Email Optin": "TRUE", "Vehicle Make": random.choice(VEHICLE_MAKES),
            "Batch Id": BATCH_ID, "Page No": 1, "Row No": i+1,
            "Load Mode": "INCREMENTAL",
            "Record Hash": record_hash(f"{cid}|{email}"),
            "Api Extracted At Date": now, "Audit Inserted At Date": now,
            "Audit Updated At Date": now, "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([cid, email, first, last])
        })
    write_csv("BREVO_CONTACT.csv", rows)

    # --- BREVO_EVENT ---
    rows = []
    for i in range(num_records):
        event_uuid = str(uuid.uuid4())
        contact_id = random.randint(1000001, 3000100)
        rows.append({
            "Contact Id": contact_id, "Event Name": random.choice(EVENT_NAMES),
            "Event Date": rand_ts(14), "Event Filter Id": random.randint(1, 20),
            "Contact Properties": json.dumps({"source": "api"}),
            "Event Properties": json.dumps({"channel": "web"}),
            "Email": rand_email(random.choice(FIRST_NAMES), random.choice(LAST_NAMES)),
            "First Name": random.choice(FIRST_NAMES), "Last Name": random.choice(LAST_NAMES),
            "Policy Number": f"POL-{random.randint(100000, 999999)}",
            "Policy Code": random.choice(POLICY_CODES),
            "Customer Id": random.randint(1000, 999999),
            "Premium": round(random.uniform(500, 25000), 2),
            "Start Date": rand_ts(30)[:10], "End Date": rand_future_date(),
            "Coverage Code": random.choice(COVERAGE_CODES),
            "Payment Gateway": random.choice(PAYMENT_GATEWAYS),
            "Payment Status Id": random.randint(1, 5),
            "Plan Id": random.randint(1, 30), "Uuid": event_uuid,
            "Batch Id": BATCH_ID, "Page No": 1, "Row No": i+1,
            "Load Mode": "INCREMENTAL", "Record Hash": record_hash(event_uuid),
            "Api Extracted At Time": now, "Audit Inserted At Time": now,
            "Audit Updated At Time": now, "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([event_uuid, contact_id])
        })
    write_csv("BREVO_EVENT.csv", rows)

    # --- BREVO_CONTACT_LISTS ---
    rows = []
    for i in range(num_records):
        list_id = 3000 + random.randint(0, 999)
        rows.append({
            "Id": list_id, "Folder Id": random.randint(1, 10),
            "Name": f"{random.choice(LIST_NAMES)} - {BATCH_ID[-8:]}",
            "Total Blacklisted": random.randint(0, 50),
            "Total Subscribers": random.randint(100, 10000),
            "Unique Subscribers": random.randint(80, 9000),
            "Batch Id": BATCH_ID, "Page No": 1, "Row No": i+1,
            "Load Mode": "INCREMENTAL", "Record Hash": record_hash(f"list_{list_id}"),
            "Api Extracted At Time": now, "Audit Inserted At Time": now,
            "Audit Updated At Time": now, "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([list_id, i])
        })
    write_csv("BREVO_CONTACT_LISTS.csv", rows)

    # --- BREVO_AGG_REPORT ---
    rows = []
    for i in range(num_records):
        day = (datetime.now() - timedelta(days=i+1)).strftime("%Y-%m-%d")
        requests = random.randint(200, 3000)
        delivered = int(requests * random.uniform(0.88, 0.97))
        opens = int(delivered * random.uniform(0.25, 0.5))
        clicks = int(opens * random.uniform(0.1, 0.35))
        rows.append({
            "Date Range": f"{day}|{day}", "Requests": requests,
            "Delivered": delivered, "Opens": opens, "Clicks": clicks,
            "Hard Bounces": random.randint(1, 20), "Soft Bounces": random.randint(1, 15),
            "Blocked": random.randint(0, 10), "Spam Reports": random.randint(0, 5),
            "Unsubscribed": random.randint(0, 10), "Unique Opens": int(opens*0.7),
            "Unique Clicks": int(clicks*0.8), "Batch Id": BATCH_ID,
            "Page No": 1, "Row No": i+1, "Load Mode": "INCREMENTAL",
            "Record Hash": record_hash(f"agg_{day}"),
            "Api Extracted At Time": now, "Audit Inserted At Time": now,
            "Audit Updated At Time": now, "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([day, requests])
        })
    write_csv("BREVO_AGG_REPORT.csv", rows)

    # --- BREVO_SMTP_EMAILS ---
    rows = []
    for i in range(num_records):
        email_uuid = str(uuid.uuid4())
        msg_id = f"<{uuid.uuid4().hex[:12]}@smtp-relay.brevo.com>"
        email_to = rand_email(random.choice(FIRST_NAMES), random.choice(LAST_NAMES))
        rows.append({
            "Message Id": msg_id, "Uuid": email_uuid,
            "SMTP Email Date": rand_ts(7), "SMTP Email": email_to,
            "SMTP From Email": "noreply@insurance-company.ae",
            "SMTP Event Email": email_to, "SMTP Event From Email": 0,
            "Tags": random.choice(["policy_confirm", "payment_receipt", "renewal_notice"]),
            "Batch Id": BATCH_ID, "Page No": 1, "Row No": i+1,
            "Load Mode": "INCREMENTAL",
            "Subject": random.choice(["Your Policy Confirmation", "Payment Received", "Renewal Reminder"]),
            "Template Id": None,
            "Audit Inserted At Time": now, "Audit Updated At Time": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([email_uuid, msg_id])
        })
    write_csv("BREVO_SMTP_EMAILS.csv", rows)

    # --- BREVO_SMTP_EVENT ---
    rows = []
    for i in range(num_records):
        msg_id = f"<{uuid.uuid4().hex[:12]}@smtp-relay.brevo.com>"
        event_type = random.choice(SMTP_EVENT_TYPES)
        reason = ""
        if event_type in ("bounced", "soft_bounced"):
            reason = random.choice(["mailbox full", "invalid address", "domain not found"])
        rows.append({
            "Message Id": msg_id, "Event": event_type,
            "SMTP Event Date": rand_ts(7),
            "SMTP Event Email": rand_email(random.choice(FIRST_NAMES), random.choice(LAST_NAMES)),
            "SMTP Event From Email": "noreply@insurance-company.ae",
            "Subject": random.choice(["Policy Update", "Payment Confirmation", "Renewal Notice"]),
            "Template Id": random.randint(100, 300), "Reason": reason,
            "Batch Id": BATCH_ID, "Page No": 1, "Row No": i+1,
            "Load Mode": "INCREMENTAL",
            "Record Hash": record_hash(f"{msg_id}|{event_type}"),
            "Api Extracted At Time": now, "Audit Inserted At Time": now,
            "Audit Updated At Time": now, "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([msg_id, event_type])
        })
    write_csv("BREVO_SMTP_EVENT.csv", rows)

    # --- PUT all CSVs to stage ---
    csv_files = ["BREVO_CONTACT.csv", "BREVO_EVENT.csv", "BREVO_CONTACT_LISTS.csv",
                 "BREVO_AGG_REPORT.csv", "BREVO_SMTP_EMAILS.csv", "BREVO_SMTP_EVENT.csv"]

    for csv_file in csv_files:
        filepath = os.path.join(OUTPUT_DIR, csv_file)
        result = session.file.put(filepath, STAGE + "/", auto_compress=False, overwrite=True)
        files_uploaded.append(f"{csv_file}: {result[0].status}")
        os.remove(filepath)

    return f"Batch {BATCH_ID} | {num_records} records/table | Files on stage: {', '.join(csv_files)}"
$$;

-- =============================================================================
-- STEP 1B: PROCEDURE - COPY from stage to Bronze tables
-- =============================================================================

CREATE OR REPLACE PROCEDURE BREVO.BRONZE.LOAD_STAGE_TO_BRONZE()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    COPY INTO BREVO.BRONZE.BREVO_AGG_REPORT
      FROM @BREVO.BRONZE.STG_BREVO/BREVO_AGG_REPORT.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_BLANK_LINES=TRUE)
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO BREVO.BRONZE.BREVO_CONTACT
      FROM @BREVO.BRONZE.STG_BREVO/BREVO_CONTACT.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_BLANK_LINES=TRUE)
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO BREVO.BRONZE.BREVO_CONTACT_LISTS
      FROM @BREVO.BRONZE.STG_BREVO/BREVO_CONTACT_LISTS.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_BLANK_LINES=TRUE)
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO BREVO.BRONZE.BREVO_EVENT
      FROM @BREVO.BRONZE.STG_BREVO/BREVO_EVENT.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_BLANK_LINES=TRUE)
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO BREVO.BRONZE.BREVO_SMTP_EMAILS
      FROM @BREVO.BRONZE.STG_BREVO/BREVO_SMTP_EMAILS.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_BLANK_LINES=TRUE)
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO BREVO.BRONZE.BREVO_SMTP_EVENT
      FROM @BREVO.BRONZE.STG_BREVO/BREVO_SMTP_EVENT.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_BLANK_LINES=TRUE)
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    RETURN 'LOAD_STAGE_TO_BRONZE completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- =============================================================================
-- STEP 2: STREAMS on Bronze tables
-- =============================================================================

CREATE OR REPLACE STREAM BREVO.BRONZE.STREAM_BREVO_CONTACT
    ON TABLE BREVO.BRONZE.BREVO_CONTACT
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM BREVO.BRONZE.STREAM_BREVO_CONTACT_LISTS
    ON TABLE BREVO.BRONZE.BREVO_CONTACT_LISTS
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM BREVO.BRONZE.STREAM_BREVO_EVENT
    ON TABLE BREVO.BRONZE.BREVO_EVENT
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM BREVO.BRONZE.STREAM_BREVO_AGG_REPORT
    ON TABLE BREVO.BRONZE.BREVO_AGG_REPORT
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM BREVO.BRONZE.STREAM_BREVO_SMTP_EMAILS
    ON TABLE BREVO.BRONZE.BREVO_SMTP_EMAILS
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM BREVO.BRONZE.STREAM_BREVO_SMTP_EVENT
    ON TABLE BREVO.BRONZE.BREVO_SMTP_EVENT
    SHOW_INITIAL_ROWS = TRUE;

-- =============================================================================
-- STEP 3: TASK DAG
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DATA GENERATION: Creates CSVs and PUTs them to stage (every 30 min)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK BREVO.BRONZE.TASK_GENERATE_TEST_DATA
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '30 MINUTE'
AS
    CALL BREVO.BRONZE.GENERATE_INCREMENTAL_DATA(4);

-- -----------------------------------------------------------------------------
-- ROOT TASK: Scheduled trigger (every 5 min)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK BREVO.SILVER.TASK_BREVO_ROOT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
AS
    SELECT 'Brevo pipeline triggered at ' || CURRENT_TIMESTAMP()::VARCHAR;

-- -----------------------------------------------------------------------------
-- STAGE → BRONZE: COPY INTO from stage files
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK BREVO.SILVER.TASK_LOAD_STAGE_TO_BRONZE
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_BREVO_ROOT
AS
    CALL BREVO.BRONZE.LOAD_STAGE_TO_BRONZE();

-- -----------------------------------------------------------------------------
-- CLEANUP: Remove stage files after load (REMOVE only works as direct task body)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK BREVO.SILVER.TASK_CLEANUP_STAGE
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_LOAD_STAGE_TO_BRONZE
AS
    REMOVE @BREVO.BRONZE.STG_BREVO;

-- -----------------------------------------------------------------------------
-- SILVER ROOT: Fires after cleanup; checks if streams have new data
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK BREVO.SILVER.TASK_SILVER_ROOT
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_CLEANUP_STAGE
    WHEN SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_CONTACT')
      OR SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_CONTACT_LISTS')
      OR SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_EVENT')
      OR SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_AGG_REPORT')
      OR SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_SMTP_EMAILS')
      OR SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_SMTP_EVENT')
AS
    SELECT 'Silver processing triggered at ' || CURRENT_TIMESTAMP()::VARCHAR;

-- -----------------------------------------------------------------------------
-- SILVER MERGE TASKS (parallel after TASK_SILVER_ROOT)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_SLV_CONTACT
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_SILVER_ROOT
AS
    CALL BREVO.SILVER.MERGE_SLV_CONTACT();

CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_SLV_CONTACT_LIST
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_SILVER_ROOT
AS
    CALL BREVO.SILVER.MERGE_SLV_CONTACT_LIST();

CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_SLV_EVENT
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_SILVER_ROOT
AS
    CALL BREVO.SILVER.MERGE_SLV_EVENT();

CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_SLV_AGG_REPORT
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_SILVER_ROOT
AS
    CALL BREVO.SILVER.MERGE_SLV_AGG_REPORT();

CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_SLV_SMTP_EMAIL
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_SILVER_ROOT
AS
    CALL BREVO.SILVER.MERGE_SLV_SMTP_EMAIL();

CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_SLV_SMTP_EVENT
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_SILVER_ROOT
AS
    CALL BREVO.SILVER.MERGE_SLV_SMTP_EVENT();

-- =============================================================================
-- GOLD LAYER TASKS
-- =============================================================================

CREATE OR REPLACE TASK BREVO.SILVER.TASK_GOLD_ROOT
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_MERGE_SLV_CONTACT,
         BREVO.SILVER.TASK_MERGE_SLV_EVENT
AS
    SELECT 'Gold pipeline triggered at ' || CURRENT_TIMESTAMP()::VARCHAR;

CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_DIM_CONTACT
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_GOLD_ROOT
AS
    CALL BREVO.GOLD.MERGE_DIM_CONTACT();

CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_DIM_POLICY
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_GOLD_ROOT
AS
    CALL BREVO.GOLD.MERGE_DIM_POLICY();

CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_DIM_PLAN
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_GOLD_ROOT
AS
    CALL BREVO.GOLD.MERGE_DIM_PLAN();

CREATE OR REPLACE TASK BREVO.SILVER.TASK_MERGE_FACT_EVENT
    WAREHOUSE = COMPUTE_WH
    AFTER BREVO.SILVER.TASK_MERGE_DIM_CONTACT,
         BREVO.SILVER.TASK_MERGE_DIM_POLICY,
         BREVO.SILVER.TASK_MERGE_DIM_PLAN
AS
    CALL BREVO.GOLD.MERGE_FACT_EVENT();

-- =============================================================================
-- STEP 4: RESUME ALL TASKS (bottom-up: leaves first, root last)
-- =============================================================================
ALTER TASK BREVO.SILVER.TASK_MERGE_FACT_EVENT RESUME;
ALTER TASK BREVO.SILVER.TASK_MERGE_DIM_CONTACT RESUME;
ALTER TASK BREVO.SILVER.TASK_MERGE_DIM_POLICY RESUME;
ALTER TASK BREVO.SILVER.TASK_MERGE_DIM_PLAN RESUME;
ALTER TASK BREVO.SILVER.TASK_GOLD_ROOT RESUME;
ALTER TASK BREVO.SILVER.TASK_MERGE_SLV_CONTACT RESUME;
ALTER TASK BREVO.SILVER.TASK_MERGE_SLV_CONTACT_LIST RESUME;
ALTER TASK BREVO.SILVER.TASK_MERGE_SLV_EVENT RESUME;
ALTER TASK BREVO.SILVER.TASK_MERGE_SLV_AGG_REPORT RESUME;
ALTER TASK BREVO.SILVER.TASK_MERGE_SLV_SMTP_EMAIL RESUME;
ALTER TASK BREVO.SILVER.TASK_MERGE_SLV_SMTP_EVENT RESUME;
ALTER TASK BREVO.SILVER.TASK_SILVER_ROOT RESUME;
ALTER TASK BREVO.SILVER.TASK_CLEANUP_STAGE RESUME;
ALTER TASK BREVO.SILVER.TASK_LOAD_STAGE_TO_BRONZE RESUME;
ALTER TASK BREVO.SILVER.TASK_BREVO_ROOT RESUME;
ALTER TASK BREVO.BRONZE.TASK_GENERATE_TEST_DATA RESUME;

-- =============================================================================
-- STEP 5: MANUAL EXECUTION (for testing)
-- =============================================================================

-- Generate test data (puts CSVs on stage):
-- CALL BREVO.BRONZE.GENERATE_INCREMENTAL_DATA(4);

-- Then trigger the pipeline manually:
-- EXECUTE TASK BREVO.SILVER.TASK_BREVO_ROOT;

-- Or do both in one go:
-- CALL BREVO.BRONZE.GENERATE_INCREMENTAL_DATA(4);
-- EXECUTE TASK BREVO.SILVER.TASK_BREVO_ROOT;

-- =============================================================================
-- MONITORING: Check task run history
-- =============================================================================
SELECT NAME, STATE, COMPLETED_TIME, NEXT_SCHEDULED_TIME, ERROR_MESSAGE
FROM TABLE(BREVO.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(MINUTE, -30, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 30
))
WHERE NAME LIKE 'TASK_%BREVO%' OR NAME LIKE 'TASK_GENERATE%' OR NAME LIKE 'TASK_LOAD%'
   OR NAME LIKE 'TASK_SILVER%' OR NAME LIKE 'TASK_GOLD%' OR NAME LIKE 'TASK_MERGE%'
   OR NAME LIKE 'TASK_CLEANUP%'
ORDER BY COMPLETED_TIME DESC;


-- =============================================================================
-- SUSPEND ALL TASKS (for maintenance)
-- =============================================================================
-- ALTER TASK BREVO.BRONZE.TASK_GENERATE_TEST_DATA SUSPEND;
-- ALTER TASK BREVO.SILVER.TASK_BREVO_ROOT SUSPEND;


-- -- Step 1: Generate CSVs and put them on stage
-- CALL BREVO.BRONZE.GENERATE_INCREMENTAL_DATA(4);

-- -- Step 2: Verify files are on stage
-- LIST @BREVO.BRONZE.STG_BREVO;

-