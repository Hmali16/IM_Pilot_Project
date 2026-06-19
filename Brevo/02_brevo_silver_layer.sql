-- Brevo Silver layer: incremental MERGE from Bronze streams into Silver tables
-- Co-authored with CoCo
-- =============================================================================
-- BREVO SOURCE | SILVER LAYER
-- Incremental MERGE via append-only streams (Bronze only receives INSERTs via COPY)
-- Source: BREVO.BRONZE → BREVO.SILVER
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS BREVO.SILVER;

-- =============================================================================
-- STEP 1: TARGET TABLES
-- =============================================================================

CREATE OR REPLACE TABLE BREVO.SILVER.SLV_CONTACT (
    CONTACT_ID              NUMBER          NOT NULL PRIMARY KEY,
    EMAIL                   VARCHAR,
    FIRST_NAME              VARCHAR,
    LAST_NAME               VARCHAR,
    DATE_OF_BIRTH           DATE,
    VEHICLE_MAKE            VARCHAR,
    IS_EMAIL_BLACKLISTED    BOOLEAN,
    IS_EMAIL_OPTIN          BOOLEAN,
    IS_SMS_BLACKLISTED      BOOLEAN,
    CREATED_AT              TIMESTAMP_NTZ,
    MODIFIED_AT             TIMESTAMP_NTZ,
    LIST_IDS_RAW            VARCHAR,
    LIST_UNSUBSCRIBED_RAW   VARCHAR,
    IS_VALID_EMAIL          BOOLEAN,
    IS_VALID_RECORD         BOOLEAN,
    API_EXTRACTED_AT        TIMESTAMP_NTZ,
    AUDIT_INSERTED_AT       TIMESTAMP_NTZ,
    AUDIT_UPDATED_AT        TIMESTAMP_NTZ,
    IS_DELETED              BOOLEAN         DEFAULT FALSE,
    BATCH_ID                VARCHAR,
    SILVER_LOADED_AT        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    SILVER_UPDATED_AT       TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE BREVO.SILVER.SLV_CONTACT_LIST (
    LIST_ID                 NUMBER          NOT NULL PRIMARY KEY,
    LIST_NAME               VARCHAR,
    FOLDER_ID               NUMBER,
    TOTAL_SUBSCRIBERS       NUMBER,
    UNIQUE_SUBSCRIBERS      NUMBER,
    TOTAL_BLACKLISTED       NUMBER,
    IS_VALID_RECORD         BOOLEAN,
    API_EXTRACTED_AT        TIMESTAMP_NTZ,
    AUDIT_INSERTED_AT       TIMESTAMP_NTZ,
    AUDIT_UPDATED_AT        TIMESTAMP_NTZ,
    IS_DELETED              BOOLEAN         DEFAULT FALSE,
    SILVER_LOADED_AT        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    SILVER_UPDATED_AT       TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE BREVO.SILVER.SLV_EVENT (
    EVENT_UUID              VARCHAR         NOT NULL PRIMARY KEY,
    CONTACT_ID              NUMBER,
    CUSTOMER_ID             NUMBER,
    EMAIL                   VARCHAR,
    FIRST_NAME              VARCHAR,
    LAST_NAME               VARCHAR,
    EVENT_NAME              VARCHAR,
    EVENT_DATE              TIMESTAMP_NTZ,
    EVENT_FILTER_ID         NUMBER,
    POLICY_CODE             VARCHAR,
    POLICY_NUMBER           VARCHAR,
    COVERAGE_CODE           VARCHAR,
    POLICY_START_DATE       DATE,
    POLICY_END_DATE         DATE,
    PREMIUM_AMOUNT          NUMBER(10,2),
    PLAN_ID                 NUMBER,
    PAYMENT_STATUS_ID       NUMBER,
    PAYMENT_GATEWAY         VARCHAR,
    CONTACT_PROPERTIES_RAW  VARCHAR,
    EVENT_PROPERTIES_RAW    VARCHAR,
    IS_VALID_RECORD         BOOLEAN,
    IS_VALID_EVENT_DATE     BOOLEAN,
    API_EXTRACTED_AT        TIMESTAMP_NTZ,
    AUDIT_INSERTED_AT       TIMESTAMP_NTZ,
    AUDIT_UPDATED_AT        TIMESTAMP_NTZ,
    IS_DELETED              BOOLEAN         DEFAULT FALSE,
    BATCH_ID                VARCHAR,
    SILVER_LOADED_AT        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    SILVER_UPDATED_AT       TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE BREVO.SILVER.SLV_AGG_REPORT (
    DATE_RANGE              VARCHAR         NOT NULL PRIMARY KEY,
    REQUESTS                NUMBER,
    DELIVERED               NUMBER,
    OPENS                   NUMBER,
    UNIQUE_OPENS            NUMBER,
    CLICKS                  NUMBER,
    UNIQUE_CLICKS           NUMBER,
    HARD_BOUNCES            NUMBER,
    SOFT_BOUNCES            NUMBER,
    BLOCKED                 NUMBER,
    SPAM_REPORTS            NUMBER,
    UNSUBSCRIBED            NUMBER,
    OPEN_RATE_PCT           NUMBER(5,2),
    CLICK_RATE_PCT          NUMBER(5,2),
    DELIVERY_RATE_PCT       NUMBER(5,2),
    BOUNCE_RATE_PCT         NUMBER(5,2),
    IS_VALID_RECORD         BOOLEAN,
    API_EXTRACTED_AT        TIMESTAMP_NTZ,
    AUDIT_INSERTED_AT       TIMESTAMP_NTZ,
    AUDIT_UPDATED_AT        TIMESTAMP_NTZ,
    IS_DELETED              BOOLEAN         DEFAULT FALSE,
    BATCH_ID                VARCHAR,
    SILVER_LOADED_AT        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    SILVER_UPDATED_AT       TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE BREVO.SILVER.SLV_SMTP_EMAIL (
    EMAIL_UUID              VARCHAR         NOT NULL PRIMARY KEY,
    MESSAGE_ID              VARCHAR,
    RECIPIENT_EMAIL         VARCHAR,
    EVENT_EMAIL             VARCHAR,
    FROM_EMAIL              VARCHAR,
    SUBJECT                 VARCHAR,
    SENT_AT                 TIMESTAMP_NTZ,
    TAGS                    VARCHAR,
    IS_VALID_RECORD         BOOLEAN,
    AUDIT_INSERTED_AT       TIMESTAMP_NTZ,
    AUDIT_UPDATED_AT        TIMESTAMP_NTZ,
    IS_DELETED              BOOLEAN         DEFAULT FALSE,
    BATCH_ID                VARCHAR,
    SILVER_LOADED_AT        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    SILVER_UPDATED_AT       TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE BREVO.SILVER.SLV_SMTP_EVENT (
    SMTP_EVENT_ID           VARCHAR         NOT NULL PRIMARY KEY,
    MESSAGE_ID              VARCHAR,
    EVENT_TYPE              VARCHAR,
    EVENT_DATE              TIMESTAMP_NTZ,
    RECIPIENT_EMAIL         VARCHAR,
    FROM_EMAIL              VARCHAR,
    SUBJECT                 VARCHAR,
    FAILURE_REASON          VARCHAR,
    TEMPLATE_ID             NUMBER,
    IS_VALID_RECORD         BOOLEAN,
    IS_VALID_EVENT_DATE     BOOLEAN,
    API_EXTRACTED_AT        TIMESTAMP_NTZ,
    AUDIT_INSERTED_AT       TIMESTAMP_NTZ,
    AUDIT_UPDATED_AT        TIMESTAMP_NTZ,
    IS_DELETED              BOOLEAN         DEFAULT FALSE,
    BATCH_ID                VARCHAR,
    SILVER_LOADED_AT        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    SILVER_UPDATED_AT       TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE BREVO.SILVER.SLV_CONTACT_LIST_MEMBERSHIP (
    CONTACT_ID              NUMBER          NOT NULL,
    LIST_ID                 NUMBER          NOT NULL,
    EMAIL                   VARCHAR,
    IS_UNSUBSCRIBED         BOOLEAN,
    SILVER_LOADED_AT        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CONTACT_ID, LIST_ID, IS_UNSUBSCRIBED)
);

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
-- STEP 3: MERGE PROCEDURES
-- Single-path: always MERGE from stream (handles both bulk & incremental)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- MERGE_SLV_CONTACT
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.SILVER.MERGE_SLV_CONTACT()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    MERGE INTO BREVO.SILVER.SLV_CONTACT AS tgt
    USING (
        SELECT
            "Id"                            AS CONTACT_ID,
            TRIM(LOWER("Email"))            AS EMAIL,
            INITCAP(TRIM("First Name"))     AS FIRST_NAME,
            INITCAP(TRIM("Last Name"))      AS LAST_NAME,
            "Dob"                           AS DATE_OF_BIRTH,
            TRIM("Vehicle Make")            AS VEHICLE_MAKE,
            "Email Blacklisted"             AS IS_EMAIL_BLACKLISTED,
            "Email Optin"                   AS IS_EMAIL_OPTIN,
            "Sms Blacklisted"              AS IS_SMS_BLACKLISTED,
            "Created At Date"               AS CREATED_AT,
            "Modified At Date"              AS MODIFIED_AT,
            TRIM("List Ids")                AS LIST_IDS_RAW,
            TRIM("List Unsubscribed")       AS LIST_UNSUBSCRIBED_RAW,
            CASE 
                WHEN "Email" IS NULL OR TRIM("Email") = '' THEN FALSE
                WHEN "Email" NOT LIKE '%_@_%.__%' THEN FALSE
                ELSE TRUE
            END                             AS IS_VALID_EMAIL,
            CASE WHEN "Id" IS NULL THEN FALSE ELSE TRUE END AS IS_VALID_RECORD,
            "Api Extracted At Date"         AS API_EXTRACTED_AT,
            "Audit Inserted At Date"        AS AUDIT_INSERTED_AT,
            "Audit Updated At Date"         AS AUDIT_UPDATED_AT,
            "Audit Is Deleted"              AS IS_DELETED,
            "Batch Id"                      AS BATCH_ID
        FROM BREVO.BRONZE.STREAM_BREVO_CONTACT
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "Id"
            ORDER BY "Modified At Date" DESC NULLS LAST, "Audit Updated At Date" DESC NULLS LAST
        ) = 1
    ) AS src
    ON tgt.CONTACT_ID = src.CONTACT_ID
    WHEN MATCHED AND src.IS_DELETED = TRUE THEN
        UPDATE SET tgt.IS_DELETED = TRUE, tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN MATCHED AND (tgt.AUDIT_UPDATED_AT < src.AUDIT_UPDATED_AT OR tgt.AUDIT_UPDATED_AT IS NULL) THEN
        UPDATE SET
            tgt.EMAIL = src.EMAIL, tgt.FIRST_NAME = src.FIRST_NAME, tgt.LAST_NAME = src.LAST_NAME,
            tgt.DATE_OF_BIRTH = src.DATE_OF_BIRTH, tgt.VEHICLE_MAKE = src.VEHICLE_MAKE,
            tgt.IS_EMAIL_BLACKLISTED = src.IS_EMAIL_BLACKLISTED, tgt.IS_EMAIL_OPTIN = src.IS_EMAIL_OPTIN,
            tgt.IS_SMS_BLACKLISTED = src.IS_SMS_BLACKLISTED, tgt.MODIFIED_AT = src.MODIFIED_AT,
            tgt.LIST_IDS_RAW = src.LIST_IDS_RAW, tgt.LIST_UNSUBSCRIBED_RAW = src.LIST_UNSUBSCRIBED_RAW,
            tgt.IS_VALID_EMAIL = src.IS_VALID_EMAIL, tgt.IS_VALID_RECORD = src.IS_VALID_RECORD,
            tgt.API_EXTRACTED_AT = src.API_EXTRACTED_AT, tgt.AUDIT_UPDATED_AT = src.AUDIT_UPDATED_AT,
            tgt.IS_DELETED = src.IS_DELETED, tgt.BATCH_ID = src.BATCH_ID,
            tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.IS_DELETED = FALSE THEN
        INSERT (CONTACT_ID, EMAIL, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, VEHICLE_MAKE,
                IS_EMAIL_BLACKLISTED, IS_EMAIL_OPTIN, IS_SMS_BLACKLISTED, CREATED_AT, MODIFIED_AT,
                LIST_IDS_RAW, LIST_UNSUBSCRIBED_RAW, IS_VALID_EMAIL, IS_VALID_RECORD,
                API_EXTRACTED_AT, AUDIT_INSERTED_AT, AUDIT_UPDATED_AT, IS_DELETED, BATCH_ID)
        VALUES (src.CONTACT_ID, src.EMAIL, src.FIRST_NAME, src.LAST_NAME, src.DATE_OF_BIRTH,
                src.VEHICLE_MAKE, src.IS_EMAIL_BLACKLISTED, src.IS_EMAIL_OPTIN, src.IS_SMS_BLACKLISTED,
                src.CREATED_AT, src.MODIFIED_AT, src.LIST_IDS_RAW, src.LIST_UNSUBSCRIBED_RAW,
                src.IS_VALID_EMAIL, src.IS_VALID_RECORD, src.API_EXTRACTED_AT, src.AUDIT_INSERTED_AT,
                src.AUDIT_UPDATED_AT, src.IS_DELETED, src.BATCH_ID);

    -- Rebuild contact-list membership
    TRUNCATE TABLE BREVO.SILVER.SLV_CONTACT_LIST_MEMBERSHIP;

    INSERT INTO BREVO.SILVER.SLV_CONTACT_LIST_MEMBERSHIP (CONTACT_ID, LIST_ID, EMAIL, IS_UNSUBSCRIBED)
    SELECT CONTACT_ID, f.VALUE::INT, EMAIL, FALSE
    FROM BREVO.SILVER.SLV_CONTACT,
        LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(LIST_IDS_RAW)) f
    WHERE LIST_IDS_RAW IS NOT NULL AND TRY_PARSE_JSON(LIST_IDS_RAW) IS NOT NULL
      AND IS_VALID_RECORD = TRUE AND IS_DELETED = FALSE;

    INSERT INTO BREVO.SILVER.SLV_CONTACT_LIST_MEMBERSHIP (CONTACT_ID, LIST_ID, EMAIL, IS_UNSUBSCRIBED)
    SELECT CONTACT_ID, f.VALUE::INT, EMAIL, TRUE
    FROM BREVO.SILVER.SLV_CONTACT,
        LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(LIST_UNSUBSCRIBED_RAW)) f
    WHERE LIST_UNSUBSCRIBED_RAW IS NOT NULL AND TRY_PARSE_JSON(LIST_UNSUBSCRIBED_RAW) IS NOT NULL
      AND IS_VALID_RECORD = TRUE AND IS_DELETED = FALSE;

    RETURN 'MERGE_SLV_CONTACT completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- -----------------------------------------------------------------------------
-- MERGE_SLV_CONTACT_LIST
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.SILVER.MERGE_SLV_CONTACT_LIST()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    MERGE INTO BREVO.SILVER.SLV_CONTACT_LIST AS tgt
    USING (
        SELECT
            "Id"                            AS LIST_ID,
            TRIM("Name")                    AS LIST_NAME,
            "Folder Id"                     AS FOLDER_ID,
            "Total Subscribers"             AS TOTAL_SUBSCRIBERS,
            "Unique Subscribers"            AS UNIQUE_SUBSCRIBERS,
            "Total Blacklisted"             AS TOTAL_BLACKLISTED,
            CASE 
                WHEN "Id" IS NULL THEN FALSE
                WHEN "Name" IS NULL OR TRIM("Name") = '' THEN FALSE
                ELSE TRUE
            END                             AS IS_VALID_RECORD,
            "Api Extracted At Time"         AS API_EXTRACTED_AT,
            "Audit Inserted At Time"        AS AUDIT_INSERTED_AT,
            "Audit Updated At Time"         AS AUDIT_UPDATED_AT,
            "Audit Is Deleted"              AS IS_DELETED
        FROM BREVO.BRONZE.STREAM_BREVO_CONTACT_LISTS
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "Id"
            ORDER BY "Audit Updated At Time" DESC NULLS LAST
        ) = 1
    ) AS src
    ON tgt.LIST_ID = src.LIST_ID
    WHEN MATCHED AND src.IS_DELETED = TRUE THEN
        UPDATE SET tgt.IS_DELETED = TRUE, tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN MATCHED AND (tgt.AUDIT_UPDATED_AT < src.AUDIT_UPDATED_AT OR tgt.AUDIT_UPDATED_AT IS NULL) THEN
        UPDATE SET
            tgt.LIST_NAME = src.LIST_NAME, tgt.FOLDER_ID = src.FOLDER_ID,
            tgt.TOTAL_SUBSCRIBERS = src.TOTAL_SUBSCRIBERS, tgt.UNIQUE_SUBSCRIBERS = src.UNIQUE_SUBSCRIBERS,
            tgt.TOTAL_BLACKLISTED = src.TOTAL_BLACKLISTED, tgt.IS_VALID_RECORD = src.IS_VALID_RECORD,
            tgt.API_EXTRACTED_AT = src.API_EXTRACTED_AT, tgt.AUDIT_UPDATED_AT = src.AUDIT_UPDATED_AT,
            tgt.IS_DELETED = src.IS_DELETED, tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.IS_DELETED = FALSE THEN
        INSERT (LIST_ID, LIST_NAME, FOLDER_ID, TOTAL_SUBSCRIBERS, UNIQUE_SUBSCRIBERS,
                TOTAL_BLACKLISTED, IS_VALID_RECORD, API_EXTRACTED_AT, AUDIT_INSERTED_AT,
                AUDIT_UPDATED_AT, IS_DELETED)
        VALUES (src.LIST_ID, src.LIST_NAME, src.FOLDER_ID, src.TOTAL_SUBSCRIBERS,
                src.UNIQUE_SUBSCRIBERS, src.TOTAL_BLACKLISTED, src.IS_VALID_RECORD,
                src.API_EXTRACTED_AT, src.AUDIT_INSERTED_AT, src.AUDIT_UPDATED_AT, src.IS_DELETED);

    RETURN 'MERGE_SLV_CONTACT_LIST completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- -----------------------------------------------------------------------------
-- MERGE_SLV_EVENT
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.SILVER.MERGE_SLV_EVENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    MERGE INTO BREVO.SILVER.SLV_EVENT AS tgt
    USING (
        SELECT
            "Uuid"                          AS EVENT_UUID,
            "Contact Id"                    AS CONTACT_ID,
            "Customer Id"                   AS CUSTOMER_ID,
            TRIM(LOWER("Email"))            AS EMAIL,
            INITCAP(TRIM("First Name"))     AS FIRST_NAME,
            INITCAP(TRIM("Last Name"))      AS LAST_NAME,
            TRIM("Event Name")              AS EVENT_NAME,
            "Event Date"                    AS EVENT_DATE,
            "Event Filter Id"               AS EVENT_FILTER_ID,
            TRIM("Policy Code")             AS POLICY_CODE,
            TRIM("Policy Number")           AS POLICY_NUMBER,
            TRIM("Coverage Code")           AS COVERAGE_CODE,
            "Start Date"                    AS POLICY_START_DATE,
            "End Date"                      AS POLICY_END_DATE,
            "Premium"                       AS PREMIUM_AMOUNT,
            "Plan Id"                       AS PLAN_ID,
            "Payment Status Id"             AS PAYMENT_STATUS_ID,
            TRIM("Payment Gateway")         AS PAYMENT_GATEWAY,
            "Contact Properties"            AS CONTACT_PROPERTIES_RAW,
            "Event Properties"              AS EVENT_PROPERTIES_RAW,
            CASE 
                WHEN "Uuid" IS NULL OR TRIM("Uuid") = '' THEN FALSE
                WHEN "Event Name" IS NULL OR TRIM("Event Name") = '' THEN FALSE
                ELSE TRUE
            END                             AS IS_VALID_RECORD,
            CASE
                WHEN "Event Date" IS NULL THEN FALSE
                WHEN "Event Date" > CURRENT_TIMESTAMP() THEN FALSE
                ELSE TRUE
            END                             AS IS_VALID_EVENT_DATE,
            "Api Extracted At Time"         AS API_EXTRACTED_AT,
            "Audit Inserted At Time"        AS AUDIT_INSERTED_AT,
            "Audit Updated At Time"         AS AUDIT_UPDATED_AT,
            "Audit Is Deleted"              AS IS_DELETED,
            "Batch Id"                      AS BATCH_ID
        FROM BREVO.BRONZE.STREAM_BREVO_EVENT
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "Uuid"
            ORDER BY "Audit Updated At Time" DESC NULLS LAST
        ) = 1
    ) AS src
    ON tgt.EVENT_UUID = src.EVENT_UUID
    WHEN MATCHED AND src.IS_DELETED = TRUE THEN
        UPDATE SET tgt.IS_DELETED = TRUE, tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN MATCHED AND (tgt.AUDIT_UPDATED_AT < src.AUDIT_UPDATED_AT OR tgt.AUDIT_UPDATED_AT IS NULL) THEN
        UPDATE SET
            tgt.CONTACT_ID = src.CONTACT_ID, tgt.CUSTOMER_ID = src.CUSTOMER_ID,
            tgt.EMAIL = src.EMAIL, tgt.FIRST_NAME = src.FIRST_NAME, tgt.LAST_NAME = src.LAST_NAME,
            tgt.EVENT_NAME = src.EVENT_NAME, tgt.EVENT_DATE = src.EVENT_DATE,
            tgt.POLICY_CODE = src.POLICY_CODE, tgt.POLICY_NUMBER = src.POLICY_NUMBER,
            tgt.COVERAGE_CODE = src.COVERAGE_CODE, tgt.POLICY_START_DATE = src.POLICY_START_DATE,
            tgt.POLICY_END_DATE = src.POLICY_END_DATE, tgt.PREMIUM_AMOUNT = src.PREMIUM_AMOUNT,
            tgt.PLAN_ID = src.PLAN_ID, tgt.PAYMENT_STATUS_ID = src.PAYMENT_STATUS_ID,
            tgt.PAYMENT_GATEWAY = src.PAYMENT_GATEWAY,
            tgt.CONTACT_PROPERTIES_RAW = src.CONTACT_PROPERTIES_RAW,
            tgt.EVENT_PROPERTIES_RAW = src.EVENT_PROPERTIES_RAW,
            tgt.IS_VALID_RECORD = src.IS_VALID_RECORD, tgt.IS_VALID_EVENT_DATE = src.IS_VALID_EVENT_DATE,
            tgt.API_EXTRACTED_AT = src.API_EXTRACTED_AT, tgt.AUDIT_UPDATED_AT = src.AUDIT_UPDATED_AT,
            tgt.IS_DELETED = src.IS_DELETED, tgt.BATCH_ID = src.BATCH_ID,
            tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.IS_DELETED = FALSE THEN
        INSERT (EVENT_UUID, CONTACT_ID, CUSTOMER_ID, EMAIL, FIRST_NAME, LAST_NAME,
                EVENT_NAME, EVENT_DATE, EVENT_FILTER_ID, POLICY_CODE, POLICY_NUMBER,
                COVERAGE_CODE, POLICY_START_DATE, POLICY_END_DATE, PREMIUM_AMOUNT,
                PLAN_ID, PAYMENT_STATUS_ID, PAYMENT_GATEWAY, CONTACT_PROPERTIES_RAW,
                EVENT_PROPERTIES_RAW, IS_VALID_RECORD, IS_VALID_EVENT_DATE,
                API_EXTRACTED_AT, AUDIT_INSERTED_AT, AUDIT_UPDATED_AT, IS_DELETED, BATCH_ID)
        VALUES (src.EVENT_UUID, src.CONTACT_ID, src.CUSTOMER_ID, src.EMAIL, src.FIRST_NAME,
                src.LAST_NAME, src.EVENT_NAME, src.EVENT_DATE, src.EVENT_FILTER_ID,
                src.POLICY_CODE, src.POLICY_NUMBER, src.COVERAGE_CODE, src.POLICY_START_DATE,
                src.POLICY_END_DATE, src.PREMIUM_AMOUNT, src.PLAN_ID, src.PAYMENT_STATUS_ID,
                src.PAYMENT_GATEWAY, src.CONTACT_PROPERTIES_RAW, src.EVENT_PROPERTIES_RAW,
                src.IS_VALID_RECORD, src.IS_VALID_EVENT_DATE, src.API_EXTRACTED_AT,
                src.AUDIT_INSERTED_AT, src.AUDIT_UPDATED_AT, src.IS_DELETED, src.BATCH_ID);

    RETURN 'MERGE_SLV_EVENT completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- -----------------------------------------------------------------------------
-- MERGE_SLV_AGG_REPORT
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.SILVER.MERGE_SLV_AGG_REPORT()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    MERGE INTO BREVO.SILVER.SLV_AGG_REPORT AS tgt
    USING (
        SELECT
            "Date Range"                    AS DATE_RANGE,
            "Requests"                      AS REQUESTS,
            "Delivered"                     AS DELIVERED,
            "Opens"                         AS OPENS,
            "Unique Opens"                  AS UNIQUE_OPENS,
            "Clicks"                        AS CLICKS,
            "Unique Clicks"                 AS UNIQUE_CLICKS,
            "Hard Bounces"                  AS HARD_BOUNCES,
            "Soft Bounces"                  AS SOFT_BOUNCES,
            "Blocked"                       AS BLOCKED,
            "Spam Reports"                  AS SPAM_REPORTS,
            "Unsubscribed"                  AS UNSUBSCRIBED,
            CASE WHEN "Delivered" > 0 THEN ROUND("Unique Opens" * 100.0 / "Delivered", 2) ELSE 0 END AS OPEN_RATE_PCT,
            CASE WHEN "Delivered" > 0 THEN ROUND("Unique Clicks" * 100.0 / "Delivered", 2) ELSE 0 END AS CLICK_RATE_PCT,
            CASE WHEN "Requests" > 0 THEN ROUND("Delivered" * 100.0 / "Requests", 2) ELSE 0 END AS DELIVERY_RATE_PCT,
            CASE WHEN "Requests" > 0 THEN ROUND(("Hard Bounces" + "Soft Bounces") * 100.0 / "Requests", 2) ELSE 0 END AS BOUNCE_RATE_PCT,
            CASE 
                WHEN "Date Range" IS NULL OR TRIM("Date Range") = '' THEN FALSE
                WHEN "Requests" < 0 OR "Delivered" < 0 THEN FALSE
                WHEN "Delivered" > "Requests" THEN FALSE
                ELSE TRUE
            END                             AS IS_VALID_RECORD,
            "Api Extracted At Time"         AS API_EXTRACTED_AT,
            "Audit Inserted At Time"        AS AUDIT_INSERTED_AT,
            "Audit Updated At Time"         AS AUDIT_UPDATED_AT,
            "Audit Is Deleted"              AS IS_DELETED,
            "Batch Id"                      AS BATCH_ID
        FROM BREVO.BRONZE.STREAM_BREVO_AGG_REPORT
        WHERE METADATA$ACTION = 'INSERT'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "Date Range"
            ORDER BY "Audit Updated At Time" DESC NULLS LAST
        ) = 1
    ) AS src
    ON tgt.DATE_RANGE = src.DATE_RANGE
    WHEN MATCHED AND src.IS_DELETED = TRUE THEN
        UPDATE SET tgt.IS_DELETED = TRUE, tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN MATCHED AND (tgt.AUDIT_UPDATED_AT < src.AUDIT_UPDATED_AT OR tgt.AUDIT_UPDATED_AT IS NULL) THEN
        UPDATE SET
            tgt.REQUESTS = src.REQUESTS, tgt.DELIVERED = src.DELIVERED,
            tgt.OPENS = src.OPENS, tgt.UNIQUE_OPENS = src.UNIQUE_OPENS,
            tgt.CLICKS = src.CLICKS, tgt.UNIQUE_CLICKS = src.UNIQUE_CLICKS,
            tgt.HARD_BOUNCES = src.HARD_BOUNCES, tgt.SOFT_BOUNCES = src.SOFT_BOUNCES,
            tgt.BLOCKED = src.BLOCKED, tgt.SPAM_REPORTS = src.SPAM_REPORTS,
            tgt.UNSUBSCRIBED = src.UNSUBSCRIBED,
            tgt.OPEN_RATE_PCT = src.OPEN_RATE_PCT, tgt.CLICK_RATE_PCT = src.CLICK_RATE_PCT,
            tgt.DELIVERY_RATE_PCT = src.DELIVERY_RATE_PCT, tgt.BOUNCE_RATE_PCT = src.BOUNCE_RATE_PCT,
            tgt.IS_VALID_RECORD = src.IS_VALID_RECORD,
            tgt.API_EXTRACTED_AT = src.API_EXTRACTED_AT, tgt.AUDIT_UPDATED_AT = src.AUDIT_UPDATED_AT,
            tgt.IS_DELETED = src.IS_DELETED, tgt.BATCH_ID = src.BATCH_ID,
            tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.IS_DELETED = FALSE THEN
        INSERT (DATE_RANGE, REQUESTS, DELIVERED, OPENS, UNIQUE_OPENS, CLICKS, UNIQUE_CLICKS,
                HARD_BOUNCES, SOFT_BOUNCES, BLOCKED, SPAM_REPORTS, UNSUBSCRIBED,
                OPEN_RATE_PCT, CLICK_RATE_PCT, DELIVERY_RATE_PCT, BOUNCE_RATE_PCT,
                IS_VALID_RECORD, API_EXTRACTED_AT, AUDIT_INSERTED_AT, AUDIT_UPDATED_AT,
                IS_DELETED, BATCH_ID)
        VALUES (src.DATE_RANGE, src.REQUESTS, src.DELIVERED, src.OPENS, src.UNIQUE_OPENS,
                src.CLICKS, src.UNIQUE_CLICKS, src.HARD_BOUNCES, src.SOFT_BOUNCES,
                src.BLOCKED, src.SPAM_REPORTS, src.UNSUBSCRIBED,
                src.OPEN_RATE_PCT, src.CLICK_RATE_PCT, src.DELIVERY_RATE_PCT, src.BOUNCE_RATE_PCT,
                src.IS_VALID_RECORD, src.API_EXTRACTED_AT, src.AUDIT_INSERTED_AT,
                src.AUDIT_UPDATED_AT, src.IS_DELETED, src.BATCH_ID);

    RETURN 'MERGE_SLV_AGG_REPORT completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- -----------------------------------------------------------------------------
-- MERGE_SLV_SMTP_EMAIL
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.SILVER.MERGE_SLV_SMTP_EMAIL()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    MERGE INTO BREVO.SILVER.SLV_SMTP_EMAIL AS tgt
    USING (
        SELECT
            "Uuid"                          AS EMAIL_UUID,
            TRIM("Message Id")              AS MESSAGE_ID,
            TRIM(LOWER("SMTP Email"))       AS RECIPIENT_EMAIL,
            TRIM(LOWER("SMTP Event Email")) AS EVENT_EMAIL,
            TRIM(LOWER("SMTP From Email"))  AS FROM_EMAIL,
            TRIM("Subject")                 AS SUBJECT,
            "SMTP Email Date"               AS SENT_AT,
            TRIM("Tags")                    AS TAGS,
            CASE 
                WHEN "Uuid" IS NULL OR TRIM("Uuid") = '' THEN FALSE
                WHEN "SMTP Email" IS NULL OR TRIM("SMTP Email") = '' THEN FALSE
                ELSE TRUE
            END                             AS IS_VALID_RECORD,
            "Audit Inserted At Time"        AS AUDIT_INSERTED_AT,
            "Audit Updated At Time"         AS AUDIT_UPDATED_AT,
            "Audit Is Deleted"              AS IS_DELETED,
            "Batch Id"                      AS BATCH_ID
        FROM BREVO.BRONZE.STREAM_BREVO_SMTP_EMAILS
        WHERE METADATA$ACTION = 'INSERT'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "Uuid"
            ORDER BY "Audit Updated At Time" DESC NULLS LAST
        ) = 1
    ) AS src
    ON tgt.EMAIL_UUID = src.EMAIL_UUID
    WHEN MATCHED AND src.IS_DELETED = TRUE THEN
        UPDATE SET tgt.IS_DELETED = TRUE, tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN MATCHED AND (tgt.AUDIT_UPDATED_AT < src.AUDIT_UPDATED_AT OR tgt.AUDIT_UPDATED_AT IS NULL) THEN
        UPDATE SET
            tgt.MESSAGE_ID = src.MESSAGE_ID, tgt.RECIPIENT_EMAIL = src.RECIPIENT_EMAIL,
            tgt.EVENT_EMAIL = src.EVENT_EMAIL, tgt.FROM_EMAIL = src.FROM_EMAIL,
            tgt.SUBJECT = src.SUBJECT, tgt.SENT_AT = src.SENT_AT, tgt.TAGS = src.TAGS,
            tgt.IS_VALID_RECORD = src.IS_VALID_RECORD, tgt.AUDIT_UPDATED_AT = src.AUDIT_UPDATED_AT,
            tgt.IS_DELETED = src.IS_DELETED, tgt.BATCH_ID = src.BATCH_ID,
            tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.IS_DELETED = FALSE THEN
        INSERT (EMAIL_UUID, MESSAGE_ID, RECIPIENT_EMAIL, EVENT_EMAIL, FROM_EMAIL,
                SUBJECT, SENT_AT, TAGS, IS_VALID_RECORD, AUDIT_INSERTED_AT,
                AUDIT_UPDATED_AT, IS_DELETED, BATCH_ID)
        VALUES (src.EMAIL_UUID, src.MESSAGE_ID, src.RECIPIENT_EMAIL, src.EVENT_EMAIL,
                src.FROM_EMAIL, src.SUBJECT, src.SENT_AT, src.TAGS, src.IS_VALID_RECORD,
                src.AUDIT_INSERTED_AT, src.AUDIT_UPDATED_AT, src.IS_DELETED, src.BATCH_ID);

    RETURN 'MERGE_SLV_SMTP_EMAIL completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- -----------------------------------------------------------------------------
-- MERGE_SLV_SMTP_EVENT
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BREVO.SILVER.MERGE_SLV_SMTP_EVENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    MERGE INTO BREVO.SILVER.SLV_SMTP_EVENT AS tgt
    USING (
        SELECT
            MD5(COALESCE("Message Id",'') || '|' || COALESCE("Event",'') || '|' || COALESCE("SMTP Event Date"::VARCHAR,'')) AS SMTP_EVENT_ID,
            TRIM("Message Id")              AS MESSAGE_ID,
            TRIM(UPPER("Event"))            AS EVENT_TYPE,
            "SMTP Event Date"               AS EVENT_DATE,
            TRIM(LOWER("SMTP Event Email")) AS RECIPIENT_EMAIL,
            TRIM(LOWER("SMTP Event From Email")) AS FROM_EMAIL,
            TRIM("Subject")                 AS SUBJECT,
            TRIM("Reason")                  AS FAILURE_REASON,
            "Template Id"                   AS TEMPLATE_ID,
            CASE 
                WHEN "Message Id" IS NULL OR TRIM("Message Id") = '' THEN FALSE
                WHEN "Event" IS NULL OR TRIM("Event") = '' THEN FALSE
                ELSE TRUE
            END                             AS IS_VALID_RECORD,
            CASE 
                WHEN "SMTP Event Date" IS NULL THEN FALSE
                WHEN "SMTP Event Date" > CURRENT_TIMESTAMP() THEN FALSE
                ELSE TRUE
            END                             AS IS_VALID_EVENT_DATE,
            "Api Extracted At Time"         AS API_EXTRACTED_AT,
            "Audit Inserted At Time"        AS AUDIT_INSERTED_AT,
            "Audit Updated At Time"         AS AUDIT_UPDATED_AT,
            "Audit Is Deleted"              AS IS_DELETED,
            "Batch Id"                      AS BATCH_ID
        FROM BREVO.BRONZE.STREAM_BREVO_SMTP_EVENT
        WHERE METADATA$ACTION = 'INSERT'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY MD5(COALESCE("Message Id",'') || '|' || COALESCE("Event",'') || '|' || COALESCE("SMTP Event Date"::VARCHAR,''))
            ORDER BY "Audit Updated At Time" DESC NULLS LAST
        ) = 1
    ) AS src
    ON tgt.SMTP_EVENT_ID = src.SMTP_EVENT_ID
    WHEN MATCHED AND src.IS_DELETED = TRUE THEN
        UPDATE SET tgt.IS_DELETED = TRUE, tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN MATCHED AND (tgt.AUDIT_UPDATED_AT < src.AUDIT_UPDATED_AT OR tgt.AUDIT_UPDATED_AT IS NULL) THEN
        UPDATE SET
            tgt.MESSAGE_ID = src.MESSAGE_ID, tgt.EVENT_TYPE = src.EVENT_TYPE,
            tgt.EVENT_DATE = src.EVENT_DATE, tgt.RECIPIENT_EMAIL = src.RECIPIENT_EMAIL,
            tgt.FROM_EMAIL = src.FROM_EMAIL, tgt.SUBJECT = src.SUBJECT,
            tgt.FAILURE_REASON = src.FAILURE_REASON, tgt.TEMPLATE_ID = src.TEMPLATE_ID,
            tgt.IS_VALID_RECORD = src.IS_VALID_RECORD, tgt.IS_VALID_EVENT_DATE = src.IS_VALID_EVENT_DATE,
            tgt.API_EXTRACTED_AT = src.API_EXTRACTED_AT, tgt.AUDIT_UPDATED_AT = src.AUDIT_UPDATED_AT,
            tgt.IS_DELETED = src.IS_DELETED, tgt.BATCH_ID = src.BATCH_ID,
            tgt.SILVER_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.IS_DELETED = FALSE THEN
        INSERT (SMTP_EVENT_ID, MESSAGE_ID, EVENT_TYPE, EVENT_DATE, RECIPIENT_EMAIL, FROM_EMAIL,
                SUBJECT, FAILURE_REASON, TEMPLATE_ID, IS_VALID_RECORD, IS_VALID_EVENT_DATE,
                API_EXTRACTED_AT, AUDIT_INSERTED_AT, AUDIT_UPDATED_AT, IS_DELETED, BATCH_ID)
        VALUES (src.SMTP_EVENT_ID, src.MESSAGE_ID, src.EVENT_TYPE, src.EVENT_DATE,
                src.RECIPIENT_EMAIL, src.FROM_EMAIL, src.SUBJECT, src.FAILURE_REASON,
                src.TEMPLATE_ID, src.IS_VALID_RECORD, src.IS_VALID_EVENT_DATE,
                src.API_EXTRACTED_AT, src.AUDIT_INSERTED_AT, src.AUDIT_UPDATED_AT,
                src.IS_DELETED, src.BATCH_ID);

    RETURN 'MERGE_SLV_SMTP_EVENT completed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;

-- =============================================================================
-- STEP 4: DATA QUALITY VIEW
-- =============================================================================
CREATE OR REPLACE VIEW BREVO.SILVER.V_DATA_QUALITY_SUMMARY AS
SELECT 'SLV_CONTACT' AS TABLE_NAME,
    COUNT(*) AS TOTAL_ROWS,
    SUM(CASE WHEN IS_VALID_RECORD AND NOT IS_DELETED THEN 1 ELSE 0 END) AS VALID_ACTIVE_ROWS,
    SUM(CASE WHEN IS_VALID_EMAIL THEN 1 ELSE 0 END) AS VALID_EMAILS,
    SUM(CASE WHEN IS_DELETED THEN 1 ELSE 0 END) AS DELETED_ROWS,
    ROUND(SUM(CASE WHEN IS_VALID_RECORD THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS VALIDITY_PCT
FROM BREVO.SILVER.SLV_CONTACT
UNION ALL
SELECT 'SLV_EVENT', COUNT(*),
    SUM(CASE WHEN IS_VALID_RECORD AND NOT IS_DELETED THEN 1 ELSE 0 END),
    SUM(CASE WHEN IS_VALID_EVENT_DATE THEN 1 ELSE 0 END),
    SUM(CASE WHEN IS_DELETED THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN IS_VALID_RECORD THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
FROM BREVO.SILVER.SLV_EVENT
UNION ALL
SELECT 'SLV_AGG_REPORT', COUNT(*),
    SUM(CASE WHEN IS_VALID_RECORD AND NOT IS_DELETED THEN 1 ELSE 0 END), NULL,
    SUM(CASE WHEN IS_DELETED THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN IS_VALID_RECORD THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
FROM BREVO.SILVER.SLV_AGG_REPORT
UNION ALL
SELECT 'SLV_SMTP_EMAIL', COUNT(*),
    SUM(CASE WHEN IS_VALID_RECORD AND NOT IS_DELETED THEN 1 ELSE 0 END), NULL,
    SUM(CASE WHEN IS_DELETED THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN IS_VALID_RECORD THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
FROM BREVO.SILVER.SLV_SMTP_EMAIL
UNION ALL
SELECT 'SLV_SMTP_EVENT', COUNT(*),
    SUM(CASE WHEN IS_VALID_RECORD AND NOT IS_DELETED THEN 1 ELSE 0 END),
    SUM(CASE WHEN IS_VALID_EVENT_DATE THEN 1 ELSE 0 END),
    SUM(CASE WHEN IS_DELETED THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN IS_VALID_RECORD THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
FROM BREVO.SILVER.SLV_SMTP_EVENT;


-- =============================================================================
-- STEP 5: EXECUTE (call procedures to load data)
-- =============================================================================
CALL BREVO.SILVER.MERGE_SLV_CONTACT();
CALL BREVO.SILVER.MERGE_SLV_CONTACT_LIST();
CALL BREVO.SILVER.MERGE_SLV_EVENT();
CALL BREVO.SILVER.MERGE_SLV_AGG_REPORT();
CALL BREVO.SILVER.MERGE_SLV_SMTP_EMAIL();
CALL BREVO.SILVER.MERGE_SLV_SMTP_EVENT();

-- Verify
SELECT * FROM BREVO.SILVER.V_DATA_QUALITY_SUMMARY;

select * from slv_contact;