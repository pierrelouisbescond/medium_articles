-- Create the training dataset from the original table --
CREATE
OR REPLACE VIEW training_dataset_selected_features AS
SELECT
    TO_TIMESTAMP_NTZ(DATE) AS DATE_NTZ,
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
OR REPLACE SNOWFLAKE.ML.FORECAST forecast_test_selected_features(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'training_dataset_selected_features'),
    TIMESTAMP_COLNAME => 'DATE_NTZ',
    TARGET_COLNAME => 'VISITS'
    );

-- Isolate exogenous features from upcoming predictions --
CREATE
OR REPLACE VIEW predictions_selected_features AS
SELECT
    TO_TIMESTAMP_NTZ(DATE) AS DATE_NTZ,
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
CALL forecast_test_selected_features ! FORECAST(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'predictions_selected_features'),
    TIMESTAMP_COLNAME => 'DATE_NTZ'
);

-- Make predictions on exogenous features --
CREATE
OR REPLACE TABLE visits_predictions_selected_features AS
SELECT * FROM TABLE (result_scan(last_query_id()));

-- explain feature importance --
CALL forecast_test_selected_features ! EXPLAIN_FEATURE_IMPORTANCE();

-- Compare the Cortex predictions and the ground truth --
SELECT
    TRTH.TS,
    TRTH.VISITS,
    ROUND(FRCST.FORECAST) AS FORECAST_ALL,
    ROUND(FRCST_AUTO.FORECAST) AS FORECAST_AUTO
FROM
    visits_truth AS TRTH
    INNER JOIN visits_predictions_all_features AS FRCST ON TRTH.TS = FRCST.TS
    INNER JOIN visits_predictions_selected_features AS FRCST_AUTO ON TRTH.TS = FRCST_AUTO.TS;

-- Create RF predictions as a view --
CREATE
OR REPLACE VIEW visits_predictions_rf AS
SELECT
    TO_TIMESTAMP_NTZ(DATE) AS TS,
    VISITS
FROM
    VISITS_PREDICTIONS_RF_SOURCE
    
-- Compare all predictions and the ground truth --
SELECT
    TRTH.TS,
    TRTH.VISITS,
    ROUND(FRCST.FORECAST) AS FORECAST_ALL,
    ROUND(FRCST_AUTO.FORECAST) AS FORECAST_AUTO,
    FRCST_RF.VISITS AS FORECAST_RF
FROM
    visits_truth AS TRTH
    INNER JOIN visits_predictions_all_features AS FRCST ON TRTH.TS = FRCST.TS
    INNER JOIN visits_predictions_selected_features AS FRCST_AUTO ON TRTH.TS = FRCST_AUTO.TS
    INNER JOIN visits_predictions_rf AS FRCST_RF ON TRTH.TS = FRCST_RF.TS;