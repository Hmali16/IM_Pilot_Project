-- Creates CLICKUP database, stage, tables (via INFER_SCHEMA), and loads CSVs from stage
-- Co-authored with CoCo
-- =============================================================================
-- IM PILOT PROJECT | CLICKUP SOURCE | BRONZE LAYER SETUP
-- Requires CSV files to be on @CLICKUP.BRONZE.STG_CLICKUP before running
-- =============================================================================

CREATE OR REPLACE DATABASE CLICKUP;

CREATE OR REPLACE SCHEMA CLICKUP.BRONZE;

-- =============================================================================
-- STAGES: CLICKUP.BRONZE
-- =============================================================================
CREATE OR REPLACE STAGE CLICKUP.BRONZE.STG_CLICKUP
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'ClickUp insurance task exports (CSV)';

-- File format for CSV ingestion
CREATE OR REPLACE FILE FORMAT CLICKUP.BRONZE.FF_CSV_CLICKUP
  TYPE = 'CSV'
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_BLANK_LINES = TRUE;

-- =============================================================================
-- CLICKUP TABLES (1 CSVs) - Created via INFER_SCHEMA
-- NOTE: CSVs must already be on @CLICKUP.BRONZE.STG_CLICKUP before running this
-- =============================================================================

CREATE OR REPLACE TABLE CLICKUP.BRONZE.CLICKUP_INSURANCE_TASKS USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA(
    LOCATION => '@CLICKUP.BRONZE.STG_CLICKUP/clickup_insurance_data.csv',
    FILE_FORMAT => 'BIRD.BRONZE.FF_CSV_INFER')));



-- =============================================================================
-- COPY INTO: CLICKUP.BRONZE (6 files)
-- =============================================================================



COPY INTO CLICKUP.BRONZE.CLICKUP_INSURANCE_TASKS
  FROM @CLICKUP.BRONZE.STG_CLICKUP/clickup_insurance_data.csv
  FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = 'CONTINUE';


-- =============================================================================
-- VERIFY: Row counts for all tables
-- =============================================================================

SELECT 'CLICKUP.BRONZE.CLICKUP_INSURANCE_TASKS', COUNT(*) FROM CLICKUP.BRONZE.CLICKUP_INSURANCE_TASKS;
