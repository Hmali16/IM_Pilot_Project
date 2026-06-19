-- Gold layer with SCD Type 2 dimensions for historical tracking and star schema fact table
-- Co-authored with CoCo
-- =============================================================================
-- IM PILOT PROJECT | BREVO SOURCE | GOLD LAYER
-- Star Schema: 1 Fact + 3 Dimensions (SCD2) + 1 DIM_DATE
--
-- DESIGN:
--   DIM_CONTACT, DIM_POLICY, DIM_PLAN → SCD Type 2 (full history)
--   FACT_EVENT → Insert/Update with surrogate key references
--
-- SCD2 means:
--   - Each dimension row has EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT
--   - When an attribute changes: old row is "closed", new row is "opened"
--   - Enables point-in-time queries and historical comparisons
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS BREVO.GOLD;

-- =============================================================================
-- STEP 1: DIMENSION TABLES (SCD Type 2)
-- =============================================================================

CREATE OR REPLACE TABLE BREVO.GOLD.DIM_CONTACT (
    DIM_CONTACT_SK          NUMBER AUTOINCREMENT PRIMARY KEY,  -- Surrogate key
    CONTACT_ID              NUMBER          NOT NULL,           -- Natural/Business key
    EMAIL                   VARCHAR,
    FIRST_NAME              VARCHAR,
    LAST_NAME               VARCHAR,
    FULL_NAME               VARCHAR,
    DATE_OF_BIRTH           DATE,
    VEHICLE_MAKE            VARCHAR,
    IS_EMAIL_BLACKLISTED    BOOLEAN,
    IS_EMAIL_OPTIN          BOOLEAN,
    IS_SMS_BLACKLISTED      BOOLEAN,
    CONTACT_CREATED_AT      TIMESTAMP_NTZ,
    -- SCD2 columns
    EFFECTIVE_START_DATE    TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_END_DATE      TIMESTAMP_NTZ   NOT NULL DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    IS_CURRENT              BOOLEAN         NOT NULL DEFAULT TRUE,
    -- Audit
    GOLD_LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    GOLD_UPDATED_AT         TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE BREVO.GOLD.DIM_DATE AS
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01')::DATE AS DATE_ID
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)
SELECT
    DATE_ID,
    YEAR(DATE_ID)           AS YEAR,
    QUARTER(DATE_ID)        AS QUARTER,
    MONTH(DATE_ID)          AS MONTH,
    MONTHNAME(DATE_ID)      AS MONTH_NAME,
    DAY(DATE_ID)            AS DAY_OF_MONTH,
    DAYNAME(DATE_ID)        AS DAY_NAME,
    CASE WHEN DAYOFWEEK(DATE_ID) IN (0, 6) THEN TRUE ELSE FALSE END AS IS_WEEKEND
FROM date_spine;

CREATE OR REPLACE TABLE BREVO.GOLD.DIM_POLICY (
    DIM_POLICY_SK           NUMBER AUTOINCREMENT PRIMARY KEY,  -- Surrogate key
    POLICY_ID               VARCHAR         NOT NULL,           -- Natural key (MD5 hash)
    POLICY_CODE             VARCHAR,
    POLICY_NUMBER           VARCHAR,
    COVERAGE_CODE           VARCHAR,
    POLICY_START_DATE       DATE,
    POLICY_END_DATE         DATE,
    POLICY_DURATION_DAYS    NUMBER,
    -- SCD2 columns
    EFFECTIVE_START_DATE    TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_END_DATE      TIMESTAMP_NTZ   NOT NULL DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    IS_CURRENT              BOOLEAN         NOT NULL DEFAULT TRUE,
    -- Audit
    GOLD_LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    GOLD_UPDATED_AT         TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE BREVO.GOLD.DIM_PLAN (
    DIM_PLAN_SK             NUMBER AUTOINCREMENT PRIMARY KEY,  -- Surrogate key
    PLAN_ID_KEY             VARCHAR         NOT NULL,           -- Natural key (MD5 hash)
    PLAN_ID                 NUMBER,
    PAYMENT_GATEWAY         VARCHAR,
    -- SCD2 columns
    EFFECTIVE_START_DATE    TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_END_DATE      TIMESTAMP_NTZ   NOT NULL DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    IS_CURRENT              BOOLEAN         NOT NULL DEFAULT TRUE,
    -- Audit
    GOLD_LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    GOLD_UPDATED_AT         TIMESTAMP_NTZ
);

-- =============================================================================
-- FACT TABLE (references dimension surrogate keys for current versions)
-- =============================================================================

CREATE OR REPLACE TABLE BREVO.GOLD.FACT_EVENT (
    EVENT_UUID              VARCHAR         NOT NULL PRIMARY KEY,
    DIM_CONTACT_SK          NUMBER,         -- FK to DIM_CONTACT (current surrogate)
    DIM_POLICY_SK           NUMBER,         -- FK to DIM_POLICY (current surrogate)
    DIM_PLAN_SK             NUMBER,         -- FK to DIM_PLAN (current surrogate)
    CONTACT_ID              NUMBER,         -- Natural key (for direct joins)
    EVENT_DATE_ID           DATE,
    POLICY_ID               VARCHAR,
    PLAN_ID_KEY             VARCHAR,
    EVENT_NAME              VARCHAR,
    PREMIUM_AMOUNT          NUMBER(10,2),
    PAYMENT_STATUS_ID       NUMBER,
    EVENT_DATE              TIMESTAMP_NTZ,
    CUSTOMER_ID             NUMBER,
    GOLD_LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    GOLD_UPDATED_AT         TIMESTAMP_NTZ
);

-- =============================================================================
-- STEP 2: STREAMS on Silver (to feed Gold layer)
-- =============================================================================

CREATE OR REPLACE STREAM BREVO.SILVER.STREAM_SLV_CONTACT
    ON TABLE BREVO.SILVER.SLV_CONTACT
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM BREVO.SILVER.STREAM_SLV_EVENT
    ON TABLE BREVO.SILVER.SLV_EVENT
    SHOW_INITIAL_ROWS = TRUE;

-- =============================================================================
-- STEP 3: SCD TYPE 2 MERGE PROCEDURES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- MERGE_DIM_CONTACT (SCD2)
-- Step 1: Close (expire) current records where tracked attributes changed
-- Step 2: Insert new current records for changed + brand new contacts
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.GOLD.MERGE_DIM_CONTACT()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    -- Step 1: Close existing current records where attributes have changed
    UPDATE BREVO.GOLD.DIM_CONTACT tgt
    SET EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(),
        IS_CURRENT = FALSE,
        GOLD_UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE tgt.IS_CURRENT = TRUE
      AND EXISTS (
          SELECT 1 FROM BREVO.SILVER.SLV_CONTACT src
          WHERE src.CONTACT_ID = tgt.CONTACT_ID
            AND src.IS_VALID_RECORD = TRUE
            AND src.IS_DELETED = FALSE
            AND (
                COALESCE(src.EMAIL, '') != COALESCE(tgt.EMAIL, '')
                OR COALESCE(src.FIRST_NAME, '') != COALESCE(tgt.FIRST_NAME, '')
                OR COALESCE(src.LAST_NAME, '') != COALESCE(tgt.LAST_NAME, '')
                OR COALESCE(src.VEHICLE_MAKE, '') != COALESCE(tgt.VEHICLE_MAKE, '')
                OR COALESCE(src.IS_EMAIL_BLACKLISTED, FALSE) != COALESCE(tgt.IS_EMAIL_BLACKLISTED, FALSE)
                OR COALESCE(src.IS_EMAIL_OPTIN, FALSE) != COALESCE(tgt.IS_EMAIL_OPTIN, FALSE)
            )
      );

    -- Step 2: Insert new current version for changed + new contacts
    INSERT INTO BREVO.GOLD.DIM_CONTACT (
        CONTACT_ID, EMAIL, FIRST_NAME, LAST_NAME, FULL_NAME,
        DATE_OF_BIRTH, VEHICLE_MAKE, IS_EMAIL_BLACKLISTED, IS_EMAIL_OPTIN,
        IS_SMS_BLACKLISTED, CONTACT_CREATED_AT,
        EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, GOLD_LOADED_AT
    )
    SELECT
        src.CONTACT_ID,
        src.EMAIL,
        src.FIRST_NAME,
        src.LAST_NAME,
        COALESCE(src.FIRST_NAME, '') || ' ' || COALESCE(src.LAST_NAME, ''),
        src.DATE_OF_BIRTH,
        src.VEHICLE_MAKE,
        src.IS_EMAIL_BLACKLISTED,
        src.IS_EMAIL_OPTIN,
        src.IS_SMS_BLACKLISTED,
        src.CREATED_AT,
        CURRENT_TIMESTAMP(),
        '9999-12-31'::TIMESTAMP_NTZ,
        TRUE,
        CURRENT_TIMESTAMP()
    FROM BREVO.SILVER.SLV_CONTACT src
    WHERE src.IS_VALID_RECORD = TRUE
      AND src.IS_DELETED = FALSE
      AND NOT EXISTS (
          SELECT 1 FROM BREVO.GOLD.DIM_CONTACT tgt
          WHERE tgt.CONTACT_ID = src.CONTACT_ID
            AND tgt.IS_CURRENT = TRUE
      );

    RETURN 'MERGE_DIM_CONTACT (SCD2) completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- -----------------------------------------------------------------------------
-- MERGE_DIM_POLICY (SCD2)
-- Tracks changes in COVERAGE_CODE, POLICY_END_DATE
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.GOLD.MERGE_DIM_POLICY()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    -- Step 1: Close records where policy attributes changed
    UPDATE BREVO.GOLD.DIM_POLICY tgt
    SET EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(),
        IS_CURRENT = FALSE,
        GOLD_UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE tgt.IS_CURRENT = TRUE
      AND EXISTS (
          SELECT 1 FROM (
              SELECT DISTINCT
                  MD5(POLICY_CODE || '|' || POLICY_NUMBER) AS POLICY_ID,
                  POLICY_CODE, POLICY_NUMBER, COVERAGE_CODE,
                  POLICY_START_DATE, POLICY_END_DATE
              FROM BREVO.SILVER.SLV_EVENT
              WHERE IS_VALID_RECORD = TRUE AND POLICY_CODE IS NOT NULL AND POLICY_NUMBER IS NOT NULL
          ) src
          WHERE src.POLICY_ID = tgt.POLICY_ID
            AND (
                COALESCE(src.COVERAGE_CODE, '') != COALESCE(tgt.COVERAGE_CODE, '')
                OR COALESCE(src.POLICY_END_DATE::VARCHAR, '') != COALESCE(tgt.POLICY_END_DATE::VARCHAR, '')
            )
      );

    -- Step 2: Insert new current records for changed + new policies
    INSERT INTO BREVO.GOLD.DIM_POLICY (
        POLICY_ID, POLICY_CODE, POLICY_NUMBER, COVERAGE_CODE,
        POLICY_START_DATE, POLICY_END_DATE, POLICY_DURATION_DAYS,
        EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, GOLD_LOADED_AT
    )
    SELECT DISTINCT
        MD5(src.POLICY_CODE || '|' || src.POLICY_NUMBER),
        src.POLICY_CODE,
        src.POLICY_NUMBER,
        src.COVERAGE_CODE,
        src.POLICY_START_DATE,
        src.POLICY_END_DATE,
        DATEDIFF(DAY, src.POLICY_START_DATE, src.POLICY_END_DATE),
        CURRENT_TIMESTAMP(),
        '9999-12-31'::TIMESTAMP_NTZ,
        TRUE,
        CURRENT_TIMESTAMP()
    FROM BREVO.SILVER.SLV_EVENT src
    WHERE src.IS_VALID_RECORD = TRUE
      AND src.POLICY_CODE IS NOT NULL
      AND src.POLICY_NUMBER IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM BREVO.GOLD.DIM_POLICY tgt
          WHERE tgt.POLICY_ID = MD5(src.POLICY_CODE || '|' || src.POLICY_NUMBER)
            AND tgt.IS_CURRENT = TRUE
      );

    RETURN 'MERGE_DIM_POLICY (SCD2) completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- -----------------------------------------------------------------------------
-- MERGE_DIM_PLAN (SCD2)
-- Tracks changes in PAYMENT_GATEWAY
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.GOLD.MERGE_DIM_PLAN()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    -- Step 1: Close records where plan attributes changed
    UPDATE BREVO.GOLD.DIM_PLAN tgt
    SET EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(),
        IS_CURRENT = FALSE,
        GOLD_UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE tgt.IS_CURRENT = TRUE
      AND EXISTS (
          SELECT 1 FROM (
              SELECT DISTINCT
                  MD5(COALESCE(PLAN_ID::VARCHAR,'') || '|' || COALESCE(PAYMENT_GATEWAY,'')) AS PLAN_ID_KEY,
                  PLAN_ID, PAYMENT_GATEWAY
              FROM BREVO.SILVER.SLV_EVENT
              WHERE IS_VALID_RECORD = TRUE AND PLAN_ID IS NOT NULL
          ) src
          WHERE src.PLAN_ID_KEY = tgt.PLAN_ID_KEY
            AND COALESCE(src.PAYMENT_GATEWAY, '') != COALESCE(tgt.PAYMENT_GATEWAY, '')
      );

    -- Step 2: Insert new current records for changed + new plans
    INSERT INTO BREVO.GOLD.DIM_PLAN (
        PLAN_ID_KEY, PLAN_ID, PAYMENT_GATEWAY,
        EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, GOLD_LOADED_AT
    )
    SELECT DISTINCT
        MD5(COALESCE(src.PLAN_ID::VARCHAR,'') || '|' || COALESCE(src.PAYMENT_GATEWAY,'')),
        src.PLAN_ID,
        src.PAYMENT_GATEWAY,
        CURRENT_TIMESTAMP(),
        '9999-12-31'::TIMESTAMP_NTZ,
        TRUE,
        CURRENT_TIMESTAMP()
    FROM BREVO.SILVER.SLV_EVENT src
    WHERE src.IS_VALID_RECORD = TRUE
      AND src.PLAN_ID IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM BREVO.GOLD.DIM_PLAN tgt
          WHERE tgt.PLAN_ID_KEY = MD5(COALESCE(src.PLAN_ID::VARCHAR,'') || '|' || COALESCE(src.PAYMENT_GATEWAY,''))
            AND tgt.IS_CURRENT = TRUE
      );

    RETURN 'MERGE_DIM_PLAN (SCD2) completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- -----------------------------------------------------------------------------
-- MERGE_FACT_EVENT
-- Links to CURRENT dimension surrogate keys (IS_CURRENT = TRUE)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.GOLD.MERGE_FACT_EVENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    MERGE INTO BREVO.GOLD.FACT_EVENT AS tgt
    USING (
        SELECT
            e.EVENT_UUID,
            e.CONTACT_ID,
            e.EVENT_DATE::DATE AS EVENT_DATE_ID,
            MD5(COALESCE(e.POLICY_CODE,'') || '|' || COALESCE(e.POLICY_NUMBER,'')) AS POLICY_ID,
            MD5(COALESCE(e.PLAN_ID::VARCHAR,'') || '|' || COALESCE(e.PAYMENT_GATEWAY,'')) AS PLAN_ID_KEY,
            e.EVENT_NAME,
            e.PREMIUM_AMOUNT,
            e.PAYMENT_STATUS_ID,
            e.EVENT_DATE,
            e.CUSTOMER_ID,
            dc.DIM_CONTACT_SK,
            dp.DIM_POLICY_SK,
            dpl.DIM_PLAN_SK
        FROM BREVO.SILVER.STREAM_SLV_EVENT e
        LEFT JOIN BREVO.GOLD.DIM_CONTACT dc
            ON e.CONTACT_ID = dc.CONTACT_ID AND dc.IS_CURRENT = TRUE
        LEFT JOIN BREVO.GOLD.DIM_POLICY dp
            ON MD5(COALESCE(e.POLICY_CODE,'') || '|' || COALESCE(e.POLICY_NUMBER,'')) = dp.POLICY_ID
            AND dp.IS_CURRENT = TRUE
        LEFT JOIN BREVO.GOLD.DIM_PLAN dpl
            ON MD5(COALESCE(e.PLAN_ID::VARCHAR,'') || '|' || COALESCE(e.PAYMENT_GATEWAY,'')) = dpl.PLAN_ID_KEY
            AND dpl.IS_CURRENT = TRUE
        WHERE e.METADATA$ACTION = 'INSERT'
          AND e.IS_VALID_RECORD = TRUE
          AND e.IS_VALID_EVENT_DATE = TRUE
          AND e.IS_DELETED = FALSE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY e.EVENT_UUID ORDER BY e.AUDIT_UPDATED_AT DESC NULLS LAST) = 1
    ) AS src
    ON tgt.EVENT_UUID = src.EVENT_UUID
    WHEN MATCHED THEN
        UPDATE SET
            tgt.DIM_CONTACT_SK = src.DIM_CONTACT_SK,
            tgt.DIM_POLICY_SK = src.DIM_POLICY_SK,
            tgt.DIM_PLAN_SK = src.DIM_PLAN_SK,
            tgt.CONTACT_ID = src.CONTACT_ID,
            tgt.EVENT_DATE_ID = src.EVENT_DATE_ID,
            tgt.POLICY_ID = src.POLICY_ID,
            tgt.PLAN_ID_KEY = src.PLAN_ID_KEY,
            tgt.EVENT_NAME = src.EVENT_NAME,
            tgt.PREMIUM_AMOUNT = src.PREMIUM_AMOUNT,
            tgt.PAYMENT_STATUS_ID = src.PAYMENT_STATUS_ID,
            tgt.EVENT_DATE = src.EVENT_DATE,
            tgt.CUSTOMER_ID = src.CUSTOMER_ID,
            tgt.GOLD_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (EVENT_UUID, DIM_CONTACT_SK, DIM_POLICY_SK, DIM_PLAN_SK,
                CONTACT_ID, EVENT_DATE_ID, POLICY_ID, PLAN_ID_KEY,
                EVENT_NAME, PREMIUM_AMOUNT, PAYMENT_STATUS_ID, EVENT_DATE, CUSTOMER_ID)
        VALUES (src.EVENT_UUID, src.DIM_CONTACT_SK, src.DIM_POLICY_SK, src.DIM_PLAN_SK,
                src.CONTACT_ID, src.EVENT_DATE_ID, src.POLICY_ID, src.PLAN_ID_KEY,
                src.EVENT_NAME, src.PREMIUM_AMOUNT, src.PAYMENT_STATUS_ID, src.EVENT_DATE,
                src.CUSTOMER_ID);

    RETURN 'MERGE_FACT_EVENT completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- =============================================================================
-- STEP 4: EXECUTE (initial load)
-- =============================================================================
CALL BREVO.GOLD.MERGE_DIM_CONTACT();
CALL BREVO.GOLD.MERGE_DIM_POLICY();
CALL BREVO.GOLD.MERGE_DIM_PLAN();
CALL BREVO.GOLD.MERGE_FACT_EVENT();

-- =============================================================================
-- STEP 5: USEFUL SCD2 QUERIES
-- =============================================================================

-- Query: Current state of all contacts (equivalent to SCD1 view)
-- SELECT * FROM BREVO.GOLD.DIM_CONTACT WHERE IS_CURRENT = TRUE;

-- Query: Full history of a specific contact
-- SELECT * FROM BREVO.GOLD.DIM_CONTACT
-- WHERE CONTACT_ID = 12345
-- ORDER BY EFFECTIVE_START_DATE;

-- Query: Point-in-time lookup (what was their email on a specific date?)
-- SELECT * FROM BREVO.GOLD.DIM_CONTACT
-- WHERE CONTACT_ID = 12345
--   AND EFFECTIVE_START_DATE <= '2026-06-01'::TIMESTAMP
--   AND EFFECTIVE_END_DATE > '2026-06-01'::TIMESTAMP;

-- Query: Compare current vs previous version (what changed?)
-- SELECT
--     curr.CONTACT_ID,
--     curr.EMAIL AS CURRENT_EMAIL, prev.EMAIL AS PREVIOUS_EMAIL,
--     curr.VEHICLE_MAKE AS CURRENT_VEHICLE, prev.VEHICLE_MAKE AS PREVIOUS_VEHICLE,
--     prev.EFFECTIVE_END_DATE AS CHANGED_AT
-- FROM BREVO.GOLD.DIM_CONTACT curr
-- JOIN BREVO.GOLD.DIM_CONTACT prev
--   ON curr.CONTACT_ID = prev.CONTACT_ID
--   AND prev.EFFECTIVE_END_DATE = curr.EFFECTIVE_START_DATE
-- WHERE curr.IS_CURRENT = TRUE;

-- Query: Contacts that changed in the last 7 days
-- SELECT CONTACT_ID, COUNT(*) AS NUM_VERSIONS
-- FROM BREVO.GOLD.DIM_CONTACT
-- WHERE EFFECTIVE_START_DATE >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
-- GROUP BY CONTACT_ID
-- HAVING COUNT(*) > 1;
