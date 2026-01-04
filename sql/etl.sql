USE WAREHOUSE cat_wh;

--- making database for project ---
CREATE DATABASE cat_walrus_db;
USE DATABASE cat_walrus_db;

--- making a schema for schema ---
CREATE SCHEMA cat_walrus_schema;
USE SCHEMA cat_walrus_schema;

--- EXTRACT getting data from dataset view in table form ---
CREATE OR REPLACE TABLE data_staging AS 
SELECT * FROM TV_METADATA_FOR_TVCM_ADVERTISING_IN_JAPAN_KANTO.PUBLIC.CM_SAMPLE;---have typo in category, it have a cateRgory---




--- a staging part, there we create a corespond for ERD tables ---

--table for company--
CREATE OR REPLACE TABLE staging_company(
    company_name STRING
);

--table for brand--
CREATE OR REPLACE TABLE staging_brand(
    brand_name STRING,
    company_name STRING
);

--table for product--
CREATE OR REPLACE TABLE staging_product(
    product_name STRING,
    brand_name STRING
);

--table for station--
CREATE OR REPLACE TABLE staging_station(
    station_id INT,
    station_name STRING,
    area INT
);

--table for commercial--
CREATE OR REPLACE TABLE staging_commercial(
    cm_base_id INT,
    product_name STRING,
    duration TIME,
    performer_name STRING,
    background_music STRING,
    narration STRING,
    situation STRING,
    memo STRING,
    note STRING,
    search_keyword STRING,
    classification STRING,
    category STRING
);

--table for logs--
CREATE OR REPLACE TABLE staging_broadcast_logs(
    cm_log_id INT,
    station_id STRING,
    start_datetime TIMESTAMP,
    end_datetime TIMESTAMP,
    first_broadcast_start TIMESTAMP,
    first_broadcast_station STRING,
    enabled_or_disabled BOOLEAN,
    cm_base_id INT
);

--table for audit--
CREATE OR REPLACE TABLE staging_log_audit(
    log_creation_datetime TIMESTAMP,
    log_update_datetime TIMESTAMP,
    cm_log_id INT
);

--table for category--
CREATE OR REPLACE TABLE staging_category(
    category STRING,
    subcategory STRING,
    subsubcategory STRING
);

--- LOAD, here a loading data into staging tables, truncate used to insure that the table clear ---

--loading company from dataset--
TRUNCATE TABLE staging_company;
INSERT INTO staging_company (company_name)
SELECT DISTINCT
    COMPANY_NAME
FROM data_staging
WHERE COMPANY_NAME IS NOT NULL;

--loading brand from dataset--
TRUNCATE TABLE staging_brand;
INSERT INTO staging_brand (brand_name, company_name)
SELECT DISTINCT
    BRAND_NAME,
    COMPANY_NAME
FROM data_staging
WHERE BRAND_NAME IS NOT NULL AND COMPANY_NAME IS NOT NULL;

TRUNCATE TABLE staging_product;
INSERT INTO staging_product (product_name, brand_name)
SELECT DISTINCT
    PRODUCT_NAME,
    BRAND_NAME
FROM data_staging
WHERE PRODUCT_NAME IS NOT NULL AND BRAND_NAME IS NOT NULL;

--loading station from dataset--
TRUNCATE TABLE staging_station;
INSERT INTO staging_station (station_id, station_name, area)
SELECT DISTINCT
    BROADCAST_STATION_ID,
    BROADCAST_STATION_NAME,
    TRY_TO_NUMBER(AREA)
FROM data_staging
WHERE BROADCAST_STATION_ID IS NOT NULL;

--loading commercial from dataset--
TRUNCATE TABLE staging_commercial;
INSERT INTO staging_commercial (cm_base_id, product_name, duration, performer_name, background_music, narration, situation, memo, note, search_keyword, classification, category)
SELECT DISTINCT
    CM_BASE_ID,
    PRODUCT_NAME,
    DURATION,
    PERFORMER_NAME,
    BACKGROUND_MUSIC,
    NARRACION,
    SITUATION,
    MEMO,
    NOTE,
    SEARCH_KEYWORD,
    CLASSIFICATION_OF_BROADCAST,
    catergory
FROM data_staging
WHERE CM_BASE_ID IS NOT NULL;

--loading logs from dataset--
TRUNCATE TABLE staging_broadcast_logs;
INSERT INTO staging_broadcast_logs (cm_log_id, station_id, start_datetime, end_datetime, first_broadcast_start, first_broadcast_station, enabled_or_disabled, cm_base_id)
SELECT DISTINCT
    CM_LOG_ID,
    BROADCAST_STATION_ID,
    BROADCAST_START_DATETIME,
    BROADCAST_END_DATETIME,
    FIRST_BROADCAST_START_DATETIME,
    FIRST_BROADCAST_STATION,
    ENABLED_OR_DISABLED,
    cm_base_id
FROM data_staging WHERE CM_LOG_ID IS NOT NULL;

--loading audit from dataset--
TRUNCATE TABLE staging_log_audit;
INSERT INTO staging_log_audit (cm_log_id, log_creation_datetime, log_update_datetime)
SELECT DISTINCT
    CM_LOG_ID,
    LOG_CREATION_DATETIME,
    LOG_UPDATE_DATETIME
FROM data_staging WHERE CM_LOG_ID IS NOT NULL;

--loading category from dataset--
TRUNCATE TABLE STAGING_CATEGORY;
INSERT INTO staging_category(category, subcategory, subsubcategory)
SELECT DISTINCT catergory, subcatergory, subsubcatergory
FROM data_staging;

--- TRANSFORM, here we transform ERD data into star schema ---

--table for station--
CREATE OR REPLACE TABLE dim_station AS
SELECT DISTINCT
    s.station_id AS broadcast_station_id,
    s.station_name AS broadcast_station_name,
    s.area AS area
FROM staging_station s;

--table for audit--
CREATE OR REPLACE TABLE dim_audit AS
SELECT DISTINCT
    a.cm_log_id as cm_log_id,
    a.log_creation_datetime as log_creation_datetime,
    a.log_update_datetime as log_update_datetime
FROM staging_log_audit a;

--table for category--
CREATE OR REPLACE TABLE dim_category AS
SELECT DISTINCT
    c.category as category,
    c.subcategory as subcategory,
    c.subsubcategory as subsubcategory
FROM staging_category c;

--table for time--
CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY full_datetime)       AS key,
    full_datetime,
    CAST(full_datetime AS DATE)                      AS date,
    DAY(full_datetime)                               AS day,
    MONTH(full_datetime)                             AS month,
    QUARTER(full_datetime)                           AS quarter,
    YEAR(full_datetime)                              AS year,
    HOUR(full_datetime)                              AS hour,
    MINUTE(full_datetime)                            AS minute
FROM (
    SELECT start_datetime AS full_datetime FROM staging_broadcast_logs WHERE start_datetime IS NOT NULL
    UNION ALL SELECT end_datetime FROM staging_broadcast_logs WHERE end_datetime IS NOT NULL
    UNION ALL SELECT first_broadcast_start FROM staging_broadcast_logs WHERE first_broadcast_start IS NOT NULL
);

--table for description--
CREATE OR REPLACE TABLE dim_description AS
SELECT DISTINCT
    c.cm_base_id,
    c.narration,
    c.situation,
    c.memo,
    c.note
FROM staging_commercial c;

--table for commercial--
CREATE OR REPLACE TABLE dim_commercial AS
SELECT DISTINCT
    c.cm_base_id,
    p.product_name,
    b.brand_name,
    co.company_name,
    c.classification,
    c.performer_name,
    c.background_music,
    c.search_keyword
FROM staging_commercial c
INNER JOIN staging_product p ON p.product_name = c.product_name
INNER JOIN staging_brand b ON b.brand_name = p.brand_name
INNER JOIN staging_company co ON co.company_name = b.company_name;

--fact table for broadcaast--
CREATE OR REPLACE TABLE fact_broadcast AS
SELECT DISTINCT
    bl.cm_log_id AS CM_LOG_ID,
    bl.enabled_or_disabled AS ENABLED_OR_DISABLED,
    c.duration AS DURATION,
    s.station_id AS STATION_ID,
    dt_start.key AS start_time_key,
    dt_end.key AS end_time_key,
    dt_first.key AS first_broadcast_time_key,
    dca.category AS category_key,
    dc.cm_base_id AS dim_commersial_key,
    la.cm_log_id AS dim_audit_key,
    dd.cm_base_id AS dim_description_key
FROM staging_commercial c
INNER JOIN staging_broadcast_logs bl ON bl.cm_base_id = c.cm_base_id 
INNER JOIN staging_station s ON s.station_id = bl.station_id
INNER JOIN dim_category dca ON dca.category = c.category
INNER JOIN dim_commercial dc ON c.cm_base_id = dc.cm_base_id
INNER JOIN dim_description dd ON c.narration = dd.narration AND c.situation = dd.situation AND c.memo = dd.memo AND c.note = dd.note
INNER JOIN dim_time dt_start ON bl.start_datetime = dt_start.full_datetime
INNER JOIN dim_time dt_end ON bl.end_datetime = dt_end.full_datetime
INNER JOIN dim_time dt_first ON bl.first_broadcast_start = dt_first.full_datetime
LEFT JOIN staging_log_audit la ON bl.cm_log_id = la.cm_log_id;