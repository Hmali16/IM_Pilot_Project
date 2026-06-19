-- Meta pipeline: Generate CSVs to stage -> COPY INTO Bronze -> Remove files -> Streams -> Silver -> Gold
-- Co-authored with CoCo
-- =============================================================================
-- IM PILOT PROJECT | META ADS SOURCE | PIPELINE ORCHESTRATION
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
-- (All DAG tasks in META.SILVER to avoid cross-schema predecessor errors.
--  Only TASK_GENERATE_TEST_DATA is in META.BRONZE as it has no children.)
--
--   META.BRONZE.TASK_GENERATE_TEST_DATA (every 30 min - generates CSVs to stage)
--       |
--   META.SILVER.TASK_META_ROOT (every 5 min)
--       +-- META.SILVER.TASK_LOAD_STAGE_TO_BRONZE (COPY INTO from stage)
--               +-- META.SILVER.TASK_CLEANUP_STAGE (REMOVE files from stage)
--                       +-- META.SILVER.TASK_META_SILVER_ROOT (checks streams)
--                               +-- TASK_MERGE_SLV_AD_ACCOUNT
--                               +-- TASK_MERGE_SLV_CAMPAIGN
--                               +-- TASK_MERGE_SLV_ADSET
--                               +-- TASK_MERGE_SLV_AD
--                               +-- TASK_MERGE_SLV_CUSTOM_AUDIENCE
--                               +-- TASK_MERGE_SLV_INSIGHT_ACCOUNT
--                               +-- TASK_MERGE_SLV_INSIGHT_CAMPAIGN
--                               +-- TASK_MERGE_SLV_INSIGHT_ADSET
--                               +-- TASK_MERGE_SLV_INSIGHT_AD
--                                       |
--                               TASK_META_GOLD_ROOT
--                                   +-- TASK_MERGE_DIM_AD_ACCOUNT
--                                   +-- TASK_MERGE_DIM_CAMPAIGN
--                                   +-- TASK_MERGE_DIM_ADSET
--                                   +-- TASK_MERGE_DIM_AD
--                                           |
--                                   TASK_MERGE_FACT_AD_INSIGHT
--
-- =============================================================================

-- =============================================================================
-- STEP 0: Ensure schemas exist and drop old tasks from META.BRONZE
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS META.BRONZE;
CREATE SCHEMA IF NOT EXISTS META.SILVER;
CREATE SCHEMA IF NOT EXISTS META.GOLD;

-- =============================================================================
-- STEP 1A: PROCEDURE - Generate CSVs and PUT to stage
-- Creates CSV files in /tmp inside Snowflake compute, then PUTs them to
-- @META.BRONZE.STG_META using session.file.put()
-- =============================================================================

CREATE OR REPLACE PROCEDURE META.BRONZE.GENERATE_INCREMENTAL_DATA(NUM_RECORDS INT)
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
    CAMPAIGN_NAMES = ["Summer Sale", "Brand Awareness Q4", "Retargeting Lapsed",
                      "Lead Gen Finance", "App Install Promo", "Holiday Special",
                      "Spring Launch", "Back to School", "Black Friday Deals"]
    ADSET_NAMES = ["US 18-35 Interest", "UK Lookalike", "Retarget Cart Abandon",
                   "High Value Segment", "Broad Audience", "Custom Audience A"]
    AD_NAMES = ["Carousel Product", "Video Testimonial", "Single Image Offer",
                "Dynamic Creative", "Story Format", "Reels Ad"]
    OBJECTIVES = ["CONVERSIONS", "LINK_CLICKS", "REACH", "BRAND_AWARENESS",
                  "APP_INSTALLS", "VIDEO_VIEWS", "LEAD_GENERATION"]
    STATUSES = ["ACTIVE", "PAUSED", "ARCHIVED"]
    BID_STRATEGIES = ["LOWEST_COST_WITHOUT_CAP", "COST_CAP", "BID_CAP"]
    OPTIMIZATION_GOALS = ["LINK_CLICKS", "CONVERSIONS", "REACH", "IMPRESSIONS",
                          "LANDING_PAGE_VIEWS", "APP_INSTALLS"]
    CTA_TYPES = ["SHOP_NOW", "LEARN_MORE", "SIGN_UP", "DOWNLOAD", "BOOK_NOW", "GET_QUOTE"]
    AUDIENCE_SUBTYPES = ["CUSTOM", "LOOKALIKE", "WEBSITE", "ENGAGEMENT", "APP_ACTIVITY"]
    DATA_SOURCE_TYPES = ["FILE", "WEBSITE", "PARTNER", "CRM", "PIXEL"]
    TIMEZONES = ["America/New_York", "America/Los_Angeles", "Europe/London", "Asia/Dubai"]
    CURRENCIES = ["USD", "GBP", "EUR", "AED"]

    BATCH_ID = f"META_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8].upper()}"
    OUTPUT_DIR = "/tmp/meta_incremental"
    STAGE = "@META.BRONZE.STG_META"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    def now_ts():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def rand_ts(days_back=30):
        dt = datetime.now() - timedelta(days=random.randint(0, days_back), hours=random.randint(0, 23))
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def rand_date(days_back=30):
        return (datetime.now() - timedelta(days=random.randint(0, days_back))).strftime("%Y-%m-%d")

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
    ad_account_id = random.randint(100000000000000, 199999999999999)

    # --- ad_account.csv ---
    rows = []
    for i in range(num_records):
        acc_id = ad_account_id + i
        rows.append({
            "Id": f"act_{acc_id}",
            "Account Id": acc_id,
            "Name": f"Ad Account {i+1}",
            "Account Status": 1,
            "Currency": random.choice(CURRENCIES),
            "Timezone Name": random.choice(TIMEZONES),
            "Spend Cap": random.randint(10000, 100000),
            "Amount Spent": random.randint(1000, 50000),
            "Balance": random.randint(0, 20000),
            "Business Id": random.randint(200000000000000, 299999999999999),
            "Business Name": f"Business_{random.randint(1, 10)}",
            "Id Brz Record": i + 1,
            "Source Name": "META_API",
            "Resource Name": "ad_account",
            "Batch Id": BATCH_ID,
            "Api Version": "v18.0",
            "Ad Account Id": acc_id,
            "Page Number": 1,
            "Audit Inserted At": now,
            "Audit Updated At": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([acc_id, i])
        })
    write_csv("ad_account.csv", rows)

    # --- campaign.csv ---
    rows = []
    campaign_ids = []
    for i in range(num_records):
        cid = 23800000000000000 + random.randint(100000, 999999)
        campaign_ids.append(cid)
        rows.append({
            "Id": cid,
            "Name": f"{random.choice(CAMPAIGN_NAMES)} {random.randint(1,99)}",
            "Status": random.choice(STATUSES),
            "Effective Status": random.choice(STATUSES),
            "Objective": random.choice(OBJECTIVES),
            "Buying Type": "AUCTION",
            "Created Time": rand_ts(90),
            "Start Time": rand_ts(30),
            "Stop Time": "",
            "Updated Time": now,
            "Daily Budget": random.randint(500, 5000),
            "Lifetime Budget": random.randint(10000, 200000),
            "Budget Remaining": "FALSE",
            "Special Ad Categories": "[]",
            "Id Brz Record": i + 1,
            "Source Name": "META_API",
            "Resource Name": "campaign",
            "Batch Id": BATCH_ID,
            "Api Version": "v18.0",
            "Ad Account Id": ad_account_id,
            "Page Number": 1,
            "Audit Inserted At": now,
            "Audit Updated At": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([cid, i])
        })
    write_csv("campaign.csv", rows)

    # --- adset.csv ---
    rows = []
    adset_ids = []
    for i in range(num_records):
        asid = 23900000000000000 + random.randint(100000, 999999)
        adset_ids.append(asid)
        rows.append({
            "Id": asid,
            "Name": f"{random.choice(ADSET_NAMES)} {random.randint(1,50)}",
            "Campaign Id": random.choice(campaign_ids),
            "Status": random.choice(STATUSES),
            "Effective Status": random.choice(STATUSES),
            "Bid Strategy": random.choice(BID_STRATEGIES),
            "Bid Amount": random.randint(100, 2000),
            "Billing Event": "IMPRESSIONS",
            "Optimization Goal": random.choice(OPTIMIZATION_GOALS),
            "Destination Type": random.choice(["WEBSITE", "APP", "MESSENGER"]),
            "Daily Budget": random.randint(500, 3000),
            "Lifetime Budget": random.randint(10000, 80000),
            "Start Time": rand_ts(30),
            "End Time": (datetime.now() + timedelta(days=random.randint(7, 90))).strftime("%Y-%m-%d %H:%M:%S"),
            "Updated Time": now,
            "Targeting": json.dumps({"geo_locations": {"countries": ["US", "GB"]}}),
            "Promoted Object": json.dumps({"page_id": str(random.randint(100000, 999999))}),
            "Id Brz Record": i + 1,
            "Source Name": "META_API",
            "Resource Name": "adset",
            "Batch Id": BATCH_ID,
            "Api Version": "v18.0",
            "Ad Account Id": ad_account_id,
            "Page Number": 1,
            "Audit Inserted At": now,
            "Audit Updated At": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([asid, i])
        })
    write_csv("adset.csv", rows)

    # --- ads.csv ---
    rows = []
    for i in range(num_records):
        aid = 24000000000000000 + random.randint(100000, 999999)
        rows.append({
            "Id": aid,
            "Name": f"{random.choice(AD_NAMES)} {random.randint(1,200)}",
            "Adset Id": random.choice(adset_ids),
            "Campaign Id": random.choice(campaign_ids),
            "Status": random.choice(STATUSES),
            "Effective Status": random.choice(STATUSES),
            "Created Time": rand_ts(60),
            "Updated Time": now,
            "Creative Id": 30000000000000000 + random.randint(1, 999999),
            "Creative Name": f"Creative_{random.randint(1,100)}",
            "Creative Title": f"Shop Now - Offer {random.randint(1,50)}",
            "Creative Body": "Get the best deals today! Limited time offer.",
            "Creative Image Url": f"https://scontent.xx.fbcdn.net/v/img_{random.randint(1000,9999)}.jpg",
            "Creative Cta Type": random.choice(CTA_TYPES),
            "Id Brz Record": i + 1,
            "Source Name": "META_API",
            "Resource Name": "ads",
            "Batch Id": BATCH_ID,
            "Api Version": "v18.0",
            "Ad Account Id": ad_account_id,
            "Page Number": 1,
            "Audit Inserted At": now,
            "Audit Updated At": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([aid, i])
        })
    write_csv("ads.csv", rows)

    # --- cutom_audience.csv (keeping original filename) ---
    rows = []
    for i in range(num_records):
        caud_id = 40000000000000000 + random.randint(100000, 999999)
        rows.append({
            "Id": caud_id,
            "Name": f"Audience_{random.choice(['Buyers','Visitors','Engaged','Lookalike','CRM'])}_{random.randint(1,50)}",
            "Description": "Custom audience for targeting",
            "Subtype": random.choice(AUDIENCE_SUBTYPES),
            "Approx Count Lower": random.randint(1000, 50000),
            "Approx Count Upper": random.randint(50000, 500000),
            "Retention Days": random.randint(30, 180),
            "Delivery Status Code": 200,
            "Delivery Status Desc": "ready",
            "Data Source Type": random.choice(DATA_SOURCE_TYPES),
            "Data Source Sub Type": "ANYTHING",
            "Lookalike Spec": "",
            "Id Brz Record": i + 1,
            "Source Name": "META_API",
            "Resource Name": "custom_audience",
            "Batch Id": BATCH_ID,
            "Api Version": "v18.0",
            "Ad Account Id": ad_account_id,
            "Page Number": 1,
            "Audit Inserted At": now,
            "Audit Updated At": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([caud_id, i])
        })
    write_csv("cutom_audience.csv", rows)

    # --- insight_account.csv ---
    rows = []
    for i in range(num_records):
        d = rand_date(30)
        impressions = random.randint(5000, 200000)
        reach = int(impressions * random.uniform(0.5, 0.9))
        clicks = int(impressions * random.uniform(0.01, 0.05))
        spend = round(random.uniform(50, 5000), 2)
        rows.append({
            "Date Start": d,
            "Date Stop": d,
            "Impressions": impressions,
            "Reach": reach,
            "Clicks": clicks,
            "Spend": spend,
            "Cpc": round(spend / max(clicks, 1), 2),
            "Cpm": round((spend / max(impressions, 1)) * 1000, 2),
            "Ctr": round((clicks / max(impressions, 1)) * 100, 4),
            "Frequency": round(impressions / max(reach, 1), 2),
            "Actions": "",
            "Action Values": "",
            "Id Brz Record": i + 1,
            "Source Name": "META_API",
            "Resource Name": "insight_account",
            "Batch Id": BATCH_ID,
            "Api Version": "v18.0",
            "Ad Account Id": ad_account_id,
            "Page Number": 1,
            "Audit Inserted At": now,
            "Audit Updated At": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([d, impressions, spend])
        })
    write_csv("insight_account.csv", rows)

    # --- insight_campaign.csv ---
    rows = []
    for i in range(num_records):
        d = rand_date(30)
        cid = random.choice(campaign_ids)
        impressions = random.randint(2000, 100000)
        reach = int(impressions * random.uniform(0.5, 0.9))
        clicks = int(impressions * random.uniform(0.01, 0.05))
        spend = round(random.uniform(20, 3000), 2)
        rows.append({
            "Campaign Id": cid,
            "Campaign Name": f"Campaign_{random.randint(1,100)}",
            "Date Start": d,
            "Date Stop": d,
            "Impressions": impressions,
            "Reach": reach,
            "Clicks": clicks,
            "Spend": spend,
            "Cpc": round(spend / max(clicks, 1), 2),
            "Cpm": round((spend / max(impressions, 1)) * 1000, 2),
            "Ctr": round((clicks / max(impressions, 1)) * 100, 4),
            "Actions": "",
            "Id Brz Record": i + 1,
            "Source Name": "META_API",
            "Resource Name": "insight_campaign",
            "Batch Id": BATCH_ID,
            "Api Version": "v18.0",
            "Ad Account Id": ad_account_id,
            "Page Number": 1,
            "Audit Inserted At": now,
            "Audit Updated At": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([cid, d, spend])
        })
    write_csv("insight_campaign.csv", rows)

    # --- insight_adset.csv ---
    rows = []
    for i in range(num_records):
        d = rand_date(30)
        asid = random.choice(adset_ids)
        impressions = random.randint(1000, 50000)
        reach = int(impressions * random.uniform(0.5, 0.9))
        clicks = int(impressions * random.uniform(0.01, 0.05))
        spend = round(random.uniform(10, 2000), 2)
        rows.append({
            "Adset Id": asid,
            "Adset Name": f"Adset_{random.randint(1,200)}",
            "Campaign Id": random.choice(campaign_ids),
            "Date Start": d,
            "Date Stop": d,
            "Impressions": impressions,
            "Reach": reach,
            "Clicks": clicks,
            "Spend": spend,
            "Cpc": round(spend / max(clicks, 1), 2),
            "Cpm": round((spend / max(impressions, 1)) * 1000, 2),
            "Ctr": round((clicks / max(impressions, 1)) * 100, 4),
            "Actions": "",
            "Id Brz Record": i + 1,
            "Source Name": "META_API",
            "Resource Name": "insight_adset",
            "Batch Id": BATCH_ID,
            "Api Version": "v18.0",
            "Ad Account Id": ad_account_id,
            "Page Number": 1,
            "Audit Inserted At": now,
            "Audit Updated At": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([asid, d, spend])
        })
    write_csv("insight_adset.csv", rows)

    # --- insight_ad.csv ---
    rows = []
    for i in range(num_records):
        d = rand_date(30)
        aid = 24000000000000000 + random.randint(100000, 999999)
        impressions = random.randint(500, 30000)
        reach = int(impressions * random.uniform(0.5, 0.9))
        clicks = int(impressions * random.uniform(0.01, 0.05))
        spend = round(random.uniform(5, 1000), 2)
        rows.append({
            "Ad Id": aid,
            "Ad Name": f"Ad_{random.randint(1,500)}",
            "Adset Id": random.choice(adset_ids),
            "Campaign Id": random.choice(campaign_ids),
            "Date Start": d,
            "Date Stop": d,
            "Impressions": impressions,
            "Reach": reach,
            "Clicks": clicks,
            "Spend": spend,
            "Cpc": round(spend / max(clicks, 1), 2),
            "Cpm": round((spend / max(impressions, 1)) * 1000, 2),
            "Ctr": round((clicks / max(impressions, 1)) * 100, 4),
            "Actions": "",
            "Id Brz Record": i + 1,
            "Source Name": "META_API",
            "Resource Name": "insight_ad",
            "Batch Id": BATCH_ID,
            "Api Version": "v18.0",
            "Ad Account Id": ad_account_id,
            "Page Number": 1,
            "Audit Inserted At": now,
            "Audit Updated At": now,
            "Audit Is Deleted": "FALSE",
            "Audit Type1 Hash": type1_hash([aid, d, spend])
        })
    write_csv("insight_ad.csv", rows)

    # --- PUT all CSVs to stage ---
    csv_files = ["ad_account.csv", "campaign.csv", "adset.csv", "ads.csv",
                 "cutom_audience.csv", "insight_account.csv", "insight_campaign.csv",
                 "insight_adset.csv", "insight_ad.csv"]

    files_uploaded = []
    for csv_file in csv_files:
        filepath = os.path.join(OUTPUT_DIR, csv_file)
        result = session.file.put(filepath, STAGE + "/", auto_compress=False, overwrite=True)
        files_uploaded.append(f"{csv_file}: {result[0].status}")
        os.remove(filepath)

    return f"Batch {BATCH_ID} | {num_records} records/table | Files: {', '.join(csv_files)}"
$$;

-- =============================================================================
-- STEP 1B: PROCEDURE - COPY from stage to Bronze tables
-- =============================================================================

CREATE OR REPLACE PROCEDURE META.BRONZE.LOAD_STAGE_TO_BRONZE()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    COPY INTO META.BRONZE.META_AD_ACCOUNT
      FROM @META.BRONZE.STG_META/ad_account.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO META.BRONZE.META_CAMPAIGN
      FROM @META.BRONZE.STG_META/campaign.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO META.BRONZE.META_ADSET
      FROM @META.BRONZE.STG_META/adset.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO META.BRONZE.META_ADS
      FROM @META.BRONZE.STG_META/ads.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO META.BRONZE.META_CUSTOM_AUDIENCE
      FROM @META.BRONZE.STG_META/cutom_audience.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO META.BRONZE.META_INSIGHT_ACCOUNT
      FROM @META.BRONZE.STG_META/insight_account.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO META.BRONZE.META_INSIGHT_CAMPAIGN
      FROM @META.BRONZE.STG_META/insight_campaign.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO META.BRONZE.META_INSIGHT_ADSET
      FROM @META.BRONZE.STG_META/insight_adset.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    COPY INTO META.BRONZE.META_INSIGHT_AD
      FROM @META.BRONZE.STG_META/insight_ad.csv
      FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';

    RETURN 'LOAD_STAGE_TO_BRONZE completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- =============================================================================
-- STEP 2: STREAMS on Bronze tables
-- =============================================================================

CREATE OR REPLACE STREAM META.BRONZE.STREAM_META_AD_ACCOUNT
    ON TABLE META.BRONZE.META_AD_ACCOUNT
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM META.BRONZE.STREAM_META_CAMPAIGN
    ON TABLE META.BRONZE.META_CAMPAIGN
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM META.BRONZE.STREAM_META_ADSET
    ON TABLE META.BRONZE.META_ADSET
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM META.BRONZE.STREAM_META_ADS
    ON TABLE META.BRONZE.META_ADS
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM META.BRONZE.STREAM_META_CUSTOM_AUDIENCE
    ON TABLE META.BRONZE.META_CUSTOM_AUDIENCE
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM META.BRONZE.STREAM_META_INSIGHT_ACCOUNT
    ON TABLE META.BRONZE.META_INSIGHT_ACCOUNT
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM META.BRONZE.STREAM_META_INSIGHT_CAMPAIGN
    ON TABLE META.BRONZE.META_INSIGHT_CAMPAIGN
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM META.BRONZE.STREAM_META_INSIGHT_ADSET
    ON TABLE META.BRONZE.META_INSIGHT_ADSET
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM META.BRONZE.STREAM_META_INSIGHT_AD
    ON TABLE META.BRONZE.META_INSIGHT_AD
    SHOW_INITIAL_ROWS = TRUE;

-- =============================================================================
-- STEP 3: TASK DAG
-- All tasks (except TASK_GENERATE_TEST_DATA) are in META.SILVER to avoid
-- cross-schema predecessor errors.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DATA GENERATION: Creates CSVs and PUTs them to stage (every 30 min)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK META.BRONZE.TASK_GENERATE_TEST_DATA
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '30 MINUTE'
AS
    CALL META.BRONZE.GENERATE_INCREMENTAL_DATA(5);

-- -----------------------------------------------------------------------------
-- ROOT TASK: Scheduled trigger (every 5 min)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK META.SILVER.TASK_META_ROOT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
AS
    SELECT 'Meta pipeline triggered at ' || CURRENT_TIMESTAMP()::VARCHAR;

-- -----------------------------------------------------------------------------
-- STAGE -> BRONZE: COPY INTO from stage files
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK META.SILVER.TASK_LOAD_STAGE_TO_BRONZE
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_ROOT
AS
    CALL META.BRONZE.LOAD_STAGE_TO_BRONZE();

-- -----------------------------------------------------------------------------
-- CLEANUP: Remove stage files after load
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK META.SILVER.TASK_CLEANUP_STAGE
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_LOAD_STAGE_TO_BRONZE
AS
    REMOVE @META.BRONZE.STG_META;

-- -----------------------------------------------------------------------------
-- SILVER ROOT: Fires after cleanup; checks if streams have new data
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK META.SILVER.TASK_META_SILVER_ROOT
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_CLEANUP_STAGE
    WHEN SYSTEM$STREAM_HAS_DATA('META.BRONZE.STREAM_META_AD_ACCOUNT')
      OR SYSTEM$STREAM_HAS_DATA('META.BRONZE.STREAM_META_CAMPAIGN')
      OR SYSTEM$STREAM_HAS_DATA('META.BRONZE.STREAM_META_ADSET')
      OR SYSTEM$STREAM_HAS_DATA('META.BRONZE.STREAM_META_ADS')
      OR SYSTEM$STREAM_HAS_DATA('META.BRONZE.STREAM_META_CUSTOM_AUDIENCE')
      OR SYSTEM$STREAM_HAS_DATA('META.BRONZE.STREAM_META_INSIGHT_ACCOUNT')
      OR SYSTEM$STREAM_HAS_DATA('META.BRONZE.STREAM_META_INSIGHT_CAMPAIGN')
      OR SYSTEM$STREAM_HAS_DATA('META.BRONZE.STREAM_META_INSIGHT_ADSET')
      OR SYSTEM$STREAM_HAS_DATA('META.BRONZE.STREAM_META_INSIGHT_AD')
AS
    SELECT 'Silver processing triggered at ' || CURRENT_TIMESTAMP()::VARCHAR;

-- -----------------------------------------------------------------------------
-- SILVER MERGE TASKS (parallel after TASK_META_SILVER_ROOT)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_SLV_AD_ACCOUNT
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_SILVER_ROOT
AS
    CALL META.SILVER.MERGE_SLV_AD_ACCOUNT();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_SLV_CAMPAIGN
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_SILVER_ROOT
AS
    CALL META.SILVER.MERGE_SLV_CAMPAIGN();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_SLV_ADSET
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_SILVER_ROOT
AS
    CALL META.SILVER.MERGE_SLV_ADSET();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_SLV_AD
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_SILVER_ROOT
AS
    CALL META.SILVER.MERGE_SLV_AD();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_SLV_CUSTOM_AUDIENCE
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_SILVER_ROOT
AS
    CALL META.SILVER.MERGE_SLV_CUSTOM_AUDIENCE();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_SLV_INSIGHT_ACCOUNT
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_SILVER_ROOT
AS
    CALL META.SILVER.MERGE_SLV_INSIGHT_ACCOUNT();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_SLV_INSIGHT_CAMPAIGN
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_SILVER_ROOT
AS
    CALL META.SILVER.MERGE_SLV_INSIGHT_CAMPAIGN();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_SLV_INSIGHT_ADSET
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_SILVER_ROOT
AS
    CALL META.SILVER.MERGE_SLV_INSIGHT_ADSET();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_SLV_INSIGHT_AD
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_SILVER_ROOT
AS
    CALL META.SILVER.MERGE_SLV_INSIGHT_AD();

-- =============================================================================
-- GOLD LAYER TASKS (also in META.SILVER to keep the DAG in one schema)
-- =============================================================================

CREATE OR REPLACE TASK META.SILVER.TASK_META_GOLD_ROOT
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_MERGE_SLV_AD_ACCOUNT,
         META.SILVER.TASK_MERGE_SLV_CAMPAIGN,
         META.SILVER.TASK_MERGE_SLV_ADSET,
         META.SILVER.TASK_MERGE_SLV_AD,
         META.SILVER.TASK_MERGE_SLV_INSIGHT_AD
AS
    SELECT 'Gold pipeline triggered at ' || CURRENT_TIMESTAMP()::VARCHAR;

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_DIM_AD_ACCOUNT
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_GOLD_ROOT
AS
    CALL META.GOLD.MERGE_DIM_AD_ACCOUNT();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_DIM_CAMPAIGN
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_GOLD_ROOT
AS
    CALL META.GOLD.MERGE_DIM_CAMPAIGN();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_DIM_ADSET
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_GOLD_ROOT
AS
    CALL META.GOLD.MERGE_DIM_ADSET();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_DIM_AD
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_META_GOLD_ROOT
AS
    CALL META.GOLD.MERGE_DIM_AD();

CREATE OR REPLACE TASK META.SILVER.TASK_MERGE_FACT_AD_INSIGHT
    WAREHOUSE = COMPUTE_WH
    AFTER META.SILVER.TASK_MERGE_DIM_AD_ACCOUNT,
         META.SILVER.TASK_MERGE_DIM_CAMPAIGN,
         META.SILVER.TASK_MERGE_DIM_ADSET,
         META.SILVER.TASK_MERGE_DIM_AD
AS
    CALL META.GOLD.MERGE_FACT_AD_INSIGHT();

-- =============================================================================
-- STEP 4: RESUME ALL TASKS (bottom-up: leaves first, root last)
-- =============================================================================
ALTER TASK META.SILVER.TASK_MERGE_FACT_AD_INSIGHT RESUME;
ALTER TASK META.SILVER.TASK_MERGE_DIM_AD_ACCOUNT RESUME;
ALTER TASK META.SILVER.TASK_MERGE_DIM_CAMPAIGN RESUME;
ALTER TASK META.SILVER.TASK_MERGE_DIM_ADSET RESUME;
ALTER TASK META.SILVER.TASK_MERGE_DIM_AD RESUME;
ALTER TASK META.SILVER.TASK_META_GOLD_ROOT RESUME;
ALTER TASK META.SILVER.TASK_MERGE_SLV_AD_ACCOUNT RESUME;
ALTER TASK META.SILVER.TASK_MERGE_SLV_CAMPAIGN RESUME;
ALTER TASK META.SILVER.TASK_MERGE_SLV_ADSET RESUME;
ALTER TASK META.SILVER.TASK_MERGE_SLV_AD RESUME;
ALTER TASK META.SILVER.TASK_MERGE_SLV_CUSTOM_AUDIENCE RESUME;
ALTER TASK META.SILVER.TASK_MERGE_SLV_INSIGHT_ACCOUNT RESUME;
ALTER TASK META.SILVER.TASK_MERGE_SLV_INSIGHT_CAMPAIGN RESUME;
ALTER TASK META.SILVER.TASK_MERGE_SLV_INSIGHT_ADSET RESUME;
ALTER TASK META.SILVER.TASK_MERGE_SLV_INSIGHT_AD RESUME;
ALTER TASK META.SILVER.TASK_META_SILVER_ROOT RESUME;
ALTER TASK META.SILVER.TASK_CLEANUP_STAGE RESUME;
ALTER TASK META.SILVER.TASK_LOAD_STAGE_TO_BRONZE RESUME;
ALTER TASK META.SILVER.TASK_META_ROOT RESUME;
ALTER TASK META.BRONZE.TASK_GENERATE_TEST_DATA RESUME;

-- =============================================================================
-- STEP 5: MANUAL EXECUTION (for testing)
-- =============================================================================

-- Generate test data (puts CSVs on stage):
CALL META.BRONZE.GENERATE_INCREMENTAL_DATA(5);

-- Then trigger the pipeline manually:
EXECUTE TASK META.SILVER.TASK_META_ROOT;

-- Or do both in one go:
CALL META.BRONZE.GENERATE_INCREMENTAL_DATA(5);
EXECUTE TASK META.SILVER.TASK_META_ROOT;

-- =============================================================================
-- MONITORING: Check task run history
-- =============================================================================
SELECT NAME, STATE, COMPLETED_TIME, NEXT_SCHEDULED_TIME, ERROR_MESSAGE
FROM TABLE(META.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(MINUTE, -30, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 30
))
WHERE NAME LIKE 'TASK_%META%' OR NAME LIKE 'TASK_GENERATE%' OR NAME LIKE 'TASK_LOAD%'
   OR NAME LIKE 'TASK_SILVER%' OR NAME LIKE 'TASK_GOLD%' OR NAME LIKE 'TASK_MERGE%'
   OR NAME LIKE 'TASK_CLEANUP%'
ORDER BY COMPLETED_TIME DESC;


-- =============================================================================
-- SUSPEND ALL TASKS (for maintenance)
-- =============================================================================
-- ALTER TASK META.BRONZE.TASK_GENERATE_TEST_DATA SUSPEND;
-- ALTER TASK META.SILVER.TASK_META_ROOT SUSPEND;


-- -- Step 1: Generate CSVs and put them on stage
-- CALL META.BRONZE.GENERATE_INCREMENTAL_DATA(5);

-- -- Step 2: Verify files are on stage
-- LIST @META.BRONZE.STG_META;

-- -- Step 3: Trigger pipeline
-- EXECUTE TASK META.SILVER.TASK_META_ROOT;
