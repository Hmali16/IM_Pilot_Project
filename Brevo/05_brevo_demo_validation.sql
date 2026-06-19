-- Demo and validation: pipeline flow, SCD1/SCD2 comparison, and row-level transformation checks
-- Co-authored with CoCo
-- =============================================================================
-- BREVO PIPELINE DEMO & VALIDATION SCRIPT
-- Run this file step-by-step to demonstrate the full data flow works.
-- =============================================================================

-- =============================================================================
-- STEP 1: INITIAL STATE
-- What: Shows current row counts across all layers, stage files, and stream status.
-- Expected Output: A table with LAYER, TABLE_NAME, ROW_COUNT for all 16 tables.
--   Stage should be empty (no files). Streams should show HAS_DATA = FALSE.
-- =============================================================================

SELECT 'BRONZE' AS LAYER, 'BREVO_CONTACT' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BREVO.BRONZE.BREVO_CONTACT
UNION ALL SELECT 'BRONZE', 'BREVO_EVENT', COUNT(*) FROM BREVO.BRONZE.BREVO_EVENT
UNION ALL SELECT 'BRONZE', 'BREVO_CONTACT_LISTS', COUNT(*) FROM BREVO.BRONZE.BREVO_CONTACT_LISTS
UNION ALL SELECT 'BRONZE', 'BREVO_AGG_REPORT', COUNT(*) FROM BREVO.BRONZE.BREVO_AGG_REPORT
UNION ALL SELECT 'BRONZE', 'BREVO_SMTP_EMAILS', COUNT(*) FROM BREVO.BRONZE.BREVO_SMTP_EMAILS
UNION ALL SELECT 'BRONZE', 'BREVO_SMTP_EVENT', COUNT(*) FROM BREVO.BRONZE.BREVO_SMTP_EVENT
UNION ALL SELECT 'SILVER', 'SLV_CONTACT', COUNT(*) FROM BREVO.SILVER.SLV_CONTACT
UNION ALL SELECT 'SILVER', 'SLV_EVENT', COUNT(*) FROM BREVO.SILVER.SLV_EVENT
UNION ALL SELECT 'SILVER', 'SLV_CONTACT_LIST', COUNT(*) FROM BREVO.SILVER.SLV_CONTACT_LIST
UNION ALL SELECT 'SILVER', 'SLV_AGG_REPORT', COUNT(*) FROM BREVO.SILVER.SLV_AGG_REPORT
UNION ALL SELECT 'SILVER', 'SLV_SMTP_EMAIL', COUNT(*) FROM BREVO.SILVER.SLV_SMTP_EMAIL
UNION ALL SELECT 'SILVER', 'SLV_SMTP_EVENT', COUNT(*) FROM BREVO.SILVER.SLV_SMTP_EVENT
UNION ALL SELECT 'GOLD', 'DIM_CONTACT', COUNT(*) FROM BREVO.GOLD.DIM_CONTACT
UNION ALL SELECT 'GOLD', 'DIM_POLICY', COUNT(*) FROM BREVO.GOLD.DIM_POLICY
UNION ALL SELECT 'GOLD', 'DIM_PLAN', COUNT(*) FROM BREVO.GOLD.DIM_PLAN
UNION ALL SELECT 'GOLD', 'FACT_EVENT', COUNT(*) FROM BREVO.GOLD.FACT_EVENT;

-- What: Lists files currently on the stage.
-- Expected Output: Empty result (no files before data generation).
LIST @BREVO.BRONZE.STG_BREVO;

-- What: Checks if any Bronze stream has unconsumed data.
-- Expected Output: All streams show HAS_DATA = FALSE (no pending changes).
SELECT
    'STREAM_BREVO_CONTACT' AS STREAM_NAME, SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_CONTACT') AS HAS_DATA
UNION ALL SELECT 'STREAM_BREVO_EVENT', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_EVENT')
UNION ALL SELECT 'STREAM_BREVO_CONTACT_LISTS', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_CONTACT_LISTS')
UNION ALL SELECT 'STREAM_BREVO_AGG_REPORT', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_AGG_REPORT')
UNION ALL SELECT 'STREAM_BREVO_SMTP_EMAILS', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_SMTP_EMAILS')
UNION ALL SELECT 'STREAM_BREVO_SMTP_EVENT', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_SMTP_EVENT');

-- =============================================================================
-- STEP 2: GENERATE DATA → STAGE
-- What: Creates 4 dummy records per table as CSVs and PUTs them to the stage.
-- Expected Output: Procedure returns batch ID. LIST shows 6 CSV files on stage.
-- =============================================================================

CALL BREVO.BRONZE.GENERATE_INCREMENTAL_DATA(4);

-- What: Confirms CSV files landed on the stage.
-- Expected Output: 6 files (BREVO_CONTACT.csv, BREVO_EVENT.csv, etc.)
LIST @BREVO.BRONZE.STG_BREVO;

-- =============================================================================
-- STEP 3: LOAD STAGE → BRONZE
-- What: COPY INTO loads CSV files from stage into Bronze tables.
-- Expected Output: Row counts increase by 4. Streams flip to HAS_DATA = TRUE.
-- =============================================================================

CALL BREVO.BRONZE.LOAD_STAGE_TO_BRONZE();

-- What: Shows updated Bronze row counts (should be previous + 4 per table).
-- Expected Output: Each table's count increased by 4.
SELECT 'BREVO_CONTACT' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BREVO.BRONZE.BREVO_CONTACT
UNION ALL SELECT 'BREVO_EVENT', COUNT(*) FROM BREVO.BRONZE.BREVO_EVENT
UNION ALL SELECT 'BREVO_CONTACT_LISTS', COUNT(*) FROM BREVO.BRONZE.BREVO_CONTACT_LISTS
UNION ALL SELECT 'BREVO_AGG_REPORT', COUNT(*) FROM BREVO.BRONZE.BREVO_AGG_REPORT
UNION ALL SELECT 'BREVO_SMTP_EMAILS', COUNT(*) FROM BREVO.BRONZE.BREVO_SMTP_EMAILS
UNION ALL SELECT 'BREVO_SMTP_EVENT', COUNT(*) FROM BREVO.BRONZE.BREVO_SMTP_EVENT
ORDER BY TABLE_NAME;

-- What: Verifies streams detected the new rows.
-- Expected Output: All streams show HAS_DATA = TRUE.
SELECT
    'STREAM_BREVO_CONTACT' AS STREAM_NAME, SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_CONTACT') AS HAS_DATA
UNION ALL SELECT 'STREAM_BREVO_EVENT', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_EVENT')
UNION ALL SELECT 'STREAM_BREVO_CONTACT_LISTS', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_CONTACT_LISTS')
UNION ALL SELECT 'STREAM_BREVO_AGG_REPORT', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_AGG_REPORT')
UNION ALL SELECT 'STREAM_BREVO_SMTP_EMAILS', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_SMTP_EMAILS')
UNION ALL SELECT 'STREAM_BREVO_SMTP_EVENT', SYSTEM$STREAM_HAS_DATA('BREVO.BRONZE.STREAM_BREVO_SMTP_EVENT');

-- =============================================================================
-- STEP 4: CLEANUP STAGE
-- What: Removes all CSV files from stage after loading.
-- Expected Output: REMOVE succeeds. LIST returns empty.
-- =============================================================================

REMOVE @BREVO.BRONZE.STG_BREVO;
LIST @BREVO.BRONZE.STG_BREVO;

-- =============================================================================
-- STEP 5: SILVER MERGES (SCD1)
-- What: Runs all 6 Silver MERGE procedures. Streams are consumed.
-- Expected Output: Silver row counts increase. Streams flip to FALSE.
-- =============================================================================

CALL BREVO.SILVER.MERGE_SLV_CONTACT();
CALL BREVO.SILVER.MERGE_SLV_CONTACT_LIST();
CALL BREVO.SILVER.MERGE_SLV_EVENT();
CALL BREVO.SILVER.MERGE_SLV_AGG_REPORT();
CALL BREVO.SILVER.MERGE_SLV_SMTP_EMAIL();
CALL BREVO.SILVER.MERGE_SLV_SMTP_EVENT();

-- What: Shows Silver row counts after merge.
-- Expected Output: Counts match Bronze unique keys (SCD1 deduplicates).
SELECT 'SLV_CONTACT' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BREVO.SILVER.SLV_CONTACT
UNION ALL SELECT 'SLV_EVENT', COUNT(*) FROM BREVO.SILVER.SLV_EVENT
UNION ALL SELECT 'SLV_CONTACT_LIST', COUNT(*) FROM BREVO.SILVER.SLV_CONTACT_LIST
UNION ALL SELECT 'SLV_AGG_REPORT', COUNT(*) FROM BREVO.SILVER.SLV_AGG_REPORT
UNION ALL SELECT 'SLV_SMTP_EMAIL', COUNT(*) FROM BREVO.SILVER.SLV_SMTP_EMAIL
UNION ALL SELECT 'SLV_SMTP_EVENT', COUNT(*) FROM BREVO.SILVER.SLV_SMTP_EVENT
ORDER BY TABLE_NAME;

-- =============================================================================
-- STEP 6: GOLD MERGES (SCD2)
-- What: Runs Gold dimension (SCD2) and fact procedures.
-- Expected Output: Gold tables populated with current + historical records.
-- =============================================================================

CALL BREVO.GOLD.MERGE_DIM_CONTACT();
CALL BREVO.GOLD.MERGE_DIM_POLICY();
CALL BREVO.GOLD.MERGE_DIM_PLAN();
CALL BREVO.GOLD.MERGE_FACT_EVENT();

-- What: Shows Gold row counts.
-- Expected Output: DIM tables may have more rows than unique keys (SCD2 history).
SELECT 'DIM_CONTACT' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BREVO.GOLD.DIM_CONTACT
UNION ALL SELECT 'DIM_POLICY', COUNT(*) FROM BREVO.GOLD.DIM_POLICY
UNION ALL SELECT 'DIM_PLAN', COUNT(*) FROM BREVO.GOLD.DIM_PLAN
UNION ALL SELECT 'FACT_EVENT', COUNT(*) FROM BREVO.GOLD.FACT_EVENT
ORDER BY TABLE_NAME;

-- =============================================================================
-- STEP 7: TASK HISTORY
-- What: Shows recent automated task executions and their status.
-- Expected Output: List of tasks with STATE=SUCCEEDED (or FAILED with error).
-- =============================================================================

SELECT NAME, STATE, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(BREVO.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 30
))
WHERE NAME LIKE 'TASK_GENERATE%' OR NAME LIKE 'TASK_LOAD%'
   OR NAME LIKE 'TASK_SILVER%' OR NAME LIKE 'TASK_GOLD%' OR NAME LIKE 'TASK_MERGE%'
   OR NAME LIKE 'TASK_CLEANUP%'
ORDER BY COMPLETED_TIME DESC;

-- =============================================================================
-- STEP 8: DATA QUALITY CHECKS
-- What: Validates data integrity across layers.
-- Expected Output: All checks should return FAILURES = 0.
-- =============================================================================

-- What: Checks for NULL primary keys in Silver (should never happen).
-- Expected Output: FAILURES = 0
SELECT 'NULL_CONTACT_IDS_IN_SILVER' AS CHECK_NAME, COUNT(*) AS FAILURES
FROM BREVO.SILVER.SLV_CONTACT WHERE CONTACT_ID IS NULL;

-- What: Checks for duplicate CONTACT_IDs in Silver (SCD1 = 1 row per key).
-- Expected Output: FAILURES = 0
SELECT 'DUPLICATE_CONTACTS_IN_SILVER' AS CHECK_NAME,
       COUNT(*) - COUNT(DISTINCT CONTACT_ID) AS FAILURES
FROM BREVO.SILVER.SLV_CONTACT;

-- What: Checks that fact table references valid dimension surrogate keys.
-- Expected Output: FAILURES = 0 (no orphan facts)
SELECT 'ORPHAN_FACTS_NO_DIM_CONTACT' AS CHECK_NAME, COUNT(*) AS FAILURES
FROM BREVO.GOLD.FACT_EVENT f
WHERE f.DIM_CONTACT_SK IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM BREVO.GOLD.DIM_CONTACT d WHERE d.DIM_CONTACT_SK = f.DIM_CONTACT_SK);

-- What: Checks that every current Gold contact exists in Silver.
-- Expected Output: FAILURES = 0
SELECT 'GOLD_CONTACT_NOT_IN_SILVER' AS CHECK_NAME, COUNT(*) AS FAILURES
FROM BREVO.GOLD.DIM_CONTACT g
LEFT JOIN BREVO.SILVER.SLV_CONTACT s ON g.CONTACT_ID = s.CONTACT_ID
WHERE g.IS_CURRENT = TRUE AND s.CONTACT_ID IS NULL;

-- =============================================================================
-- STEP 9: BRONZE → SILVER ROW-LEVEL TRANSFORMATION
-- What: Shows the SAME records in Bronze (raw) vs Silver (cleansed) to
--       demonstrate what transformations are applied.
-- Expected Output: Bronze has quoted column names, raw types, no validation.
--   Silver has clean column names, proper types, IS_VALID flags.
-- =============================================================================

-- What: Shows 5 recent contacts as they appear in Bronze (raw from CSV).
-- Expected Output: Columns like "Id", "Email", "Email Blacklisted" (text TRUE/FALSE).
SELECT
    'BRONZE' AS LAYER,
    b."Id"::VARCHAR AS ID,
    b."Email" AS EMAIL,
    b."First Name" AS FIRST_NAME,
    b."Last Name" AS LAST_NAME,
    b."Email Blacklisted"::VARCHAR AS EMAIL_BLACKLISTED,
    b."Vehicle Make" AS VEHICLE_MAKE,
    b."Dob"::VARCHAR AS DOB,
    b."Batch Id" AS BATCH_ID,
    b."Load Mode" AS LOAD_MODE,
    b."Audit Inserted At Date"::VARCHAR AS AUDIT_TS
FROM BREVO.BRONZE.BREVO_CONTACT b
ORDER BY b."Id" DESC LIMIT 5;

-- What: Shows the same contacts in Silver after transformation.
-- Expected Output: Clean column names (CONTACT_ID, IS_EMAIL_BLACKLISTED as BOOLEAN),
--   plus IS_VALID_RECORD flag and SILVER_LOADED_AT timestamp.
SELECT
    'SILVER' AS LAYER,
    s.CONTACT_ID::VARCHAR AS ID,
    s.EMAIL,
    s.FIRST_NAME,
    s.LAST_NAME,
    s.IS_EMAIL_BLACKLISTED::VARCHAR AS EMAIL_BLACKLISTED,
    s.VEHICLE_MAKE,
    s.DATE_OF_BIRTH::VARCHAR AS DOB,
    s.BATCH_ID,
    s.IS_VALID_RECORD::VARCHAR AS IS_VALID,
    s.SILVER_LOADED_AT::VARCHAR AS AUDIT_TS
FROM BREVO.SILVER.SLV_CONTACT s
ORDER BY s.CONTACT_ID DESC LIMIT 5;

-- What: Shows 5 recent events in Bronze (raw).
-- Expected Output: Raw column names like "Uuid", "Contact Id", "Premium" (number as text).
SELECT
    'BRONZE' AS LAYER,
    b."Uuid" AS EVENT_UUID,
    b."Contact Id"::VARCHAR AS CONTACT_ID,
    b."Event Name" AS EVENT_NAME,
    b."Policy Number" AS POLICY_NUMBER,
    b."Policy Code" AS POLICY_CODE,
    b."Coverage Code" AS COVERAGE_CODE,
    b."Premium"::VARCHAR AS PREMIUM,
    b."Payment Gateway" AS PAYMENT_GATEWAY,
    b."Batch Id" AS BATCH_ID
FROM BREVO.BRONZE.BREVO_EVENT b
ORDER BY b."Audit Inserted At Time" DESC LIMIT 5;

-- What: Shows same events in Silver after cleansing.
-- Expected Output: Standardized names (EVENT_UUID, PREMIUM_AMOUNT), proper types.
SELECT
    'SILVER' AS LAYER,
    s.EVENT_UUID,
    s.CONTACT_ID::VARCHAR,
    s.EVENT_NAME,
    s.POLICY_NUMBER,
    s.POLICY_CODE,
    s.COVERAGE_CODE,
    s.PREMIUM_AMOUNT::VARCHAR AS PREMIUM,
    s.PAYMENT_GATEWAY,
    s.BATCH_ID
FROM BREVO.SILVER.SLV_EVENT s
ORDER BY s.AUDIT_INSERTED_AT DESC LIMIT 5;

-- What: Lists all transformations applied from Bronze to Silver.
-- Expected Output: A reference table of TRANSITION, TRANSFORMATION, EXAMPLE.
SELECT 'BRONZE → SILVER' AS TRANSITION, 'Column Renaming' AS TRANSFORMATION,
       '"First Name" → FIRST_NAME, "Email Blacklisted" → IS_EMAIL_BLACKLISTED' AS EXAMPLE
UNION ALL SELECT 'BRONZE → SILVER', 'Type Casting',
       '"Email Blacklisted" (TEXT) → IS_EMAIL_BLACKLISTED (BOOLEAN)'
UNION ALL SELECT 'BRONZE → SILVER', 'Data Validation',
       'IS_VALID_EMAIL, IS_VALID_RECORD flags computed'
UNION ALL SELECT 'BRONZE → SILVER', 'Deduplication (SCD1)',
       'MERGE by primary key — latest record overwrites old'
UNION ALL SELECT 'BRONZE → SILVER', 'Column Rename: Premium',
       '"Premium" → PREMIUM_AMOUNT (standardized naming)';

-- =============================================================================
-- STEP 10: SILVER → GOLD ROW-LEVEL TRANSFORMATION
-- What: Shows the SAME records in Silver vs Gold to demonstrate what's added.
-- Expected Output: Silver has flat records. Gold adds surrogate keys, derived
--   columns (FULL_NAME), and SCD2 versioning columns.
-- =============================================================================

-- What: Shows 5 contacts in Silver (flat, no surrogate key, no SCD2 columns).
-- Expected Output: CONTACT_ID, EMAIL, etc. No FULL_NAME, no DIM_CONTACT_SK.
SELECT
    'SILVER' AS LAYER,
    s.CONTACT_ID::VARCHAR AS ID,
    s.EMAIL,
    s.FIRST_NAME,
    s.LAST_NAME,
    NULL AS FULL_NAME,
    s.VEHICLE_MAKE,
    s.IS_EMAIL_BLACKLISTED::VARCHAR AS BLACKLISTED,
    NULL AS SURROGATE_KEY,
    NULL AS EFFECTIVE_START,
    NULL AS IS_CURRENT
FROM BREVO.SILVER.SLV_CONTACT s
ORDER BY s.CONTACT_ID DESC LIMIT 5;

-- What: Shows same contacts in Gold (with surrogate key, FULL_NAME, SCD2 dates).
-- Expected Output: DIM_CONTACT_SK (autoincrement), FULL_NAME derived,
--   EFFECTIVE_START_DATE set, IS_CURRENT = TRUE.
SELECT
    'GOLD' AS LAYER,
    g.CONTACT_ID::VARCHAR AS ID,
    g.EMAIL,
    g.FIRST_NAME,
    g.LAST_NAME,
    g.FULL_NAME,
    g.VEHICLE_MAKE,
    g.IS_EMAIL_BLACKLISTED::VARCHAR AS BLACKLISTED,
    g.DIM_CONTACT_SK::VARCHAR AS SURROGATE_KEY,
    g.EFFECTIVE_START_DATE::VARCHAR AS EFFECTIVE_START,
    g.IS_CURRENT::VARCHAR AS IS_CURRENT
FROM BREVO.GOLD.DIM_CONTACT g
WHERE g.IS_CURRENT = TRUE
ORDER BY g.CONTACT_ID DESC LIMIT 5;

-- What: Shows 5 events in Silver vs Gold Fact to see enrichment.
-- Expected Output: Silver has no surrogate keys. Gold FACT has DIM_CONTACT_SK,
--   DIM_POLICY_SK, DIM_PLAN_SK (joined from dimensions).
SELECT
    'SILVER' AS LAYER,
    s.EVENT_UUID,
    s.CONTACT_ID::VARCHAR AS CONTACT_ID,
    s.EVENT_NAME,
    s.POLICY_NUMBER,
    s.PREMIUM_AMOUNT::VARCHAR AS PREMIUM,
    NULL AS DIM_CONTACT_SK,
    NULL AS DIM_POLICY_SK,
    s.SILVER_LOADED_AT::VARCHAR AS LOADED_AT
FROM BREVO.SILVER.SLV_EVENT s
ORDER BY s.AUDIT_INSERTED_AT DESC LIMIT 5;

SELECT
    'GOLD' AS LAYER,
    f.EVENT_UUID,
    f.CONTACT_ID::VARCHAR,
    f.EVENT_NAME,
    f.POLICY_ID AS POLICY_KEY,
    f.PREMIUM_AMOUNT::VARCHAR AS PREMIUM,
    f.DIM_CONTACT_SK::VARCHAR,
    f.DIM_POLICY_SK::VARCHAR,
    f.GOLD_LOADED_AT::VARCHAR AS LOADED_AT
FROM BREVO.GOLD.FACT_EVENT f
ORDER BY f.GOLD_LOADED_AT DESC;

-- What: Lists all transformations applied from Silver to Gold.
-- Expected Output: A reference table showing surrogate keys, derived columns,
--   SCD2 versioning, filtering, and fact enrichment.
SELECT 'SILVER → GOLD' AS TRANSITION, 'Surrogate Key' AS TRANSFORMATION,
       'DIM_CONTACT_SK (autoincrement) replaces natural CONTACT_ID for joins' AS EXAMPLE
UNION ALL SELECT 'SILVER → GOLD', 'Derived Column',
       'FULL_NAME = FIRST_NAME || LAST_NAME (computed in Gold)'
UNION ALL SELECT 'SILVER → GOLD', 'SCD2 Versioning',
       'EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT added'
UNION ALL SELECT 'SILVER → GOLD', 'Filtering',
       'Only IS_VALID_RECORD=TRUE, IS_DELETED=FALSE pass to Gold'
UNION ALL SELECT 'SILVER → GOLD', 'Fact Enrichment',
       'FACT_EVENT gets DIM_CONTACT_SK, DIM_POLICY_SK, DIM_PLAN_SK via joins';

-- =============================================================================
-- STEP 11: SCD TYPE 1 DEMO (Silver - Overwrite, No History)
-- What: Demonstrates that Silver uses SCD1 — when a record is updated,
--   the old values are completely overwritten. No history is kept.
-- Expected Output: After merge, the contact shows new email/vehicle.
--   Only 1 row exists per CONTACT_ID (no duplicates).
-- =============================================================================

-- What: Shows a contact's current state BEFORE the update.
-- Expected Output: Original email and vehicle for the latest CONTACT_ID.
SELECT CONTACT_ID, EMAIL, VEHICLE_MAKE, BATCH_ID, SILVER_UPDATED_AT
FROM BREVO.SILVER.SLV_CONTACT
ORDER BY CONTACT_ID DESC LIMIT 3;

-- What: Inserts a "changed" record into Bronze (same Id, new email + vehicle).
-- Expected Output: 1 row inserted. This simulates a source system update.
INSERT INTO BREVO.BRONZE.BREVO_CONTACT
SELECT "Id",
       'scd1_demo_' || "Id" || '@changed.com' AS "Email",
       "Email Blacklisted", "Sms Blacklisted",
       "Created At Date", CURRENT_TIMESTAMP() AS "Modified At Date",
       "List Ids", "List Unsubscribed", "Attributes",
       "First Name", "Last Name", "Dob", "Email Optin",
       'Porsche' AS "Vehicle Make",
       'SCD1_DEMO' AS "Batch Id",
       "Page No", "Row No", 'UPDATE' AS "Load Mode",
       "Record Hash", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
       CURRENT_TIMESTAMP(), FALSE, "Audit Type1 Hash"
FROM BREVO.BRONZE.BREVO_CONTACT
ORDER BY "Id" DESC LIMIT 1;

-- What: Runs Silver MERGE — this will OVERWRITE the old record (SCD1 behavior).
-- Expected Output: Procedure returns success message.
CALL BREVO.SILVER.MERGE_SLV_CONTACT();

-- What: Shows the contact AFTER merge — old email/vehicle are GONE.
-- Expected Output: Email = 'scd1_demo_...' and Vehicle = 'Porsche'.
--   SILVER_UPDATED_AT is newer than SILVER_LOADED_AT.
SELECT CONTACT_ID, EMAIL, VEHICLE_MAKE, BATCH_ID, SILVER_UPDATED_AT
FROM BREVO.SILVER.SLV_CONTACT
WHERE BATCH_ID = 'SCD1_DEMO' OR EMAIL LIKE 'scd1_demo_%'
ORDER BY SILVER_UPDATED_AT DESC LIMIT 3;

-- What: PROOF that SCD1 keeps only 1 row per key (no history).
-- Expected Output: 0 rows returned (no duplicates exist).
SELECT CONTACT_ID, COUNT(*) AS ROW_COUNT
FROM BREVO.SILVER.SLV_CONTACT
GROUP BY CONTACT_ID HAVING COUNT(*) > 1 LIMIT 5;

-- =============================================================================
-- STEP 12: SCD TYPE 2 DEMO (Gold - Full History Preserved)
-- What: Demonstrates that Gold uses SCD2 — when a record changes, the old
--   version is "closed" (IS_CURRENT=FALSE, EFFECTIVE_END_DATE=NOW) and a new
--   version is "opened" (IS_CURRENT=TRUE). History is preserved.
-- Expected Output: Multiple rows per CONTACT_ID in Gold. Can query
--   point-in-time state and compare current vs previous values.
-- =============================================================================

-- What: Shows current state in Gold BEFORE the SCD2 merge.
-- Expected Output: All rows have IS_CURRENT=TRUE, EFFECTIVE_END_DATE='9999-12-31'.
SELECT DIM_CONTACT_SK, CONTACT_ID, EMAIL, VEHICLE_MAKE,
       IS_CURRENT, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE
FROM BREVO.GOLD.DIM_CONTACT
WHERE IS_CURRENT = TRUE
ORDER BY CONTACT_ID DESC LIMIT 5;

-- What: Runs Gold MERGE (SCD2). This will:
--   1. Close the old record (IS_CURRENT=FALSE, EFFECTIVE_END_DATE=NOW)
--   2. Insert a new current record with updated email/vehicle from Silver
-- Expected Output: Procedure returns success message.
CALL BREVO.GOLD.MERGE_DIM_CONTACT();

-- What: Shows ALL versions of contacts that have history (multiple rows per key).
-- Expected Output: For the updated contact: one row with IS_CURRENT=FALSE (old)
--   and one with IS_CURRENT=TRUE (new). Different EMAIL and VEHICLE_MAKE values.
SELECT DIM_CONTACT_SK, CONTACT_ID, EMAIL, VEHICLE_MAKE,
       IS_CURRENT, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE
FROM BREVO.GOLD.DIM_CONTACT
WHERE CONTACT_ID IN (
    SELECT CONTACT_ID FROM BREVO.GOLD.DIM_CONTACT
    GROUP BY CONTACT_ID HAVING COUNT(*) > 1
)
ORDER BY CONTACT_ID, EFFECTIVE_START_DATE
LIMIT 10;

-- What: Shows contacts with version history and their version counts.
-- Expected Output: TOTAL_VERSIONS > 1, CURRENT = 1, HISTORICAL >= 1.
SELECT CONTACT_ID,
       COUNT(*) AS TOTAL_VERSIONS,
       SUM(CASE WHEN IS_CURRENT THEN 1 ELSE 0 END) AS CURRENT_VERSIONS,
       SUM(CASE WHEN NOT IS_CURRENT THEN 1 ELSE 0 END) AS HISTORICAL_VERSIONS
FROM BREVO.GOLD.DIM_CONTACT
GROUP BY CONTACT_ID HAVING COUNT(*) > 1
ORDER BY TOTAL_VERSIONS DESC LIMIT 10;

-- What: Compares current vs previous version side-by-side for same contact.
-- Expected Output: Shows PREVIOUS_EMAIL vs CURRENT_EMAIL, PREVIOUS_VEHICLE vs
--   CURRENT_VEHICLE, and CHANGED_AT timestamp.
SELECT
    curr.CONTACT_ID,
    prev.EMAIL AS PREVIOUS_EMAIL,
    curr.EMAIL AS CURRENT_EMAIL,
    prev.VEHICLE_MAKE AS PREVIOUS_VEHICLE,
    curr.VEHICLE_MAKE AS CURRENT_VEHICLE,
    prev.EFFECTIVE_END_DATE AS CHANGED_AT
FROM BREVO.GOLD.DIM_CONTACT curr
JOIN BREVO.GOLD.DIM_CONTACT prev
  ON curr.CONTACT_ID = prev.CONTACT_ID
  AND prev.IS_CURRENT = FALSE
  AND curr.IS_CURRENT = TRUE
ORDER BY prev.EFFECTIVE_END_DATE DESC LIMIT 5;

-- What: Point-in-time query — what was this contact's data 1 hour ago?
-- Expected Output: The version that was active 1 hour ago (may differ from current).
SELECT CONTACT_ID, EMAIL, VEHICLE_MAKE, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE
FROM BREVO.GOLD.DIM_CONTACT
WHERE CONTACT_ID IN (
    SELECT CONTACT_ID FROM BREVO.GOLD.DIM_CONTACT GROUP BY CONTACT_ID HAVING COUNT(*) > 1
)
AND EFFECTIVE_START_DATE <= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
AND EFFECTIVE_END_DATE > DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
LIMIT 5;

-- =============================================================================
-- STEP 13: SCD1 vs SCD2 SIDE-BY-SIDE SUMMARY
-- What: Final comparison of Silver (SCD1) vs Gold (SCD2) behavior.
-- Expected Output: Silver has TOTAL_ROWS = UNIQUE_CONTACTS (no history).
--   Gold has TOTAL_ROWS > UNIQUE_CONTACTS (extra rows = historical versions).
-- =============================================================================

-- What: Record counts comparison showing SCD1 has no history, SCD2 does.
-- Expected Output: Silver HISTORICAL_ROWS = 0. Gold HISTORICAL_ROWS > 0.
SELECT
    'SILVER (SCD1)' AS LAYER,
    COUNT(*) AS TOTAL_ROWS,
    COUNT(DISTINCT CONTACT_ID) AS UNIQUE_CONTACTS,
    COUNT(*) - COUNT(DISTINCT CONTACT_ID) AS HISTORICAL_ROWS
FROM BREVO.SILVER.SLV_CONTACT
UNION ALL
SELECT
    'GOLD (SCD2)',
    COUNT(*),
    COUNT(DISTINCT CONTACT_ID),
    COUNT(*) - COUNT(DISTINCT CONTACT_ID)
FROM BREVO.GOLD.DIM_CONTACT;


