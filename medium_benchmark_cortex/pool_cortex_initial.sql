-- Create the training dataset from the original table --
CREATE
OR REPLACE VIEW training_dataset_all_features AS
SELECT
    TO_TIMESTAMP_NTZ(DATE) AS DATE_NTZ,
    YEAR,
    MONTH,
    DAYOFMONTH,
    WEEKDAY_LABEL,
    WEEKDAY,
    NATIONAL_HOLIDAYS,
    SCHOOL_HOLIDAYS,
    VENT_MOY,
    TEMP_MOY,
    PLUIE_MOY,
    OPENING_TIME,
    VISITS
FROM
    pool
WHERE
    DATE >= '2022-01-13'
    AND DATE < '2023-11-19';
-- Build Forecasting model --
    CREATE
    OR REPLACE SNOWFLAKE.ML.FORECAST forecast_test_all_features(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'training_dataset_all_features'),
        TIMESTAMP_COLNAME => 'DATE_NTZ',
        TARGET_COLNAME => 'VISITS'
    );

-- Isolate exogenous features from upcoming predictions --
CREATE
OR REPLACE VIEW predictions_all_features AS
SELECT
    TO_TIMESTAMP_NTZ(DATE) AS DATE_NTZ,
    YEAR,
    MONTH,
    DAYOFMONTH,
    WEEKDAY_LABEL,
    WEEKDAY,
    NATIONAL_HOLIDAYS,
    SCHOOL_HOLIDAYS,
    VENT_MOY,
    TEMP_MOY,
    PLUIE_MOY,
    OPENING_TIME
FROM
    pool
WHERE
    DATE >= '2023-11-19'
    AND DATE < '2023-12-03';

-- Make predictions on exogenous features --
    CALL forecast_test_all_features ! FORECAST(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'predictions_all_features'),
        TIMESTAMP_COLNAME => 'DATE_NTZ'
    );

-- Make predictions on exogenous features --
CREATE
OR REPLACE TABLE visits_predictions_all_features AS
SELECT
    *
FROM
    TABLE (result_scan(last_query_id()));
-- Display predictions --
SELECT
    *
FROM
    visits_predictions_all_features;

-- explain feature importance --
CALL forecast_test_all_features ! EXPLAIN_FEATURE_IMPORTANCE();

-- Extract the true number of visits --
CREATE
OR REPLACE VIEW visits_truth AS
SELECT
    TO_TIMESTAMP_NTZ(DATE) AS TS,
    VISITS
FROM
    pool
WHERE
    DATE >= '2023-11-19'
    AND DATE < '2023-12-03';

-- Compare the predictions and the ground truth --
SELECT
    FRCST.TS,
    ROUND(FRCST.FORECAST) AS FORECAST,
    TRTH.VISITS
FROM
    visits_predictions_all_features AS FRCST
    INNER JOIN visits_truth AS TRTH ON FRCST.TS = TRTH.TS;