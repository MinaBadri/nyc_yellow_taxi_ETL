--Create final Table in processed schema
CREATE OR REPLACE TABLE yellow_taxi (
    VendorID INTEGER,
    VendorID_String VARCHAR,
    vendor_name VARCHAR AS (
        CASE 
            WHEN VendorID = 1 THEN 'Creative Mobile Technologies' 
            WHEN VendorID = 2 THEN 'VeriFone Inc' 
            ELSE VendorID_String  -- Fallback to string representation if available
        END
    ),
    pickup_datetime TIMESTAMP_NTZ,
    dropoff_datetime TIMESTAMP_NTZ,
    start_latitude FLOAT,
    start_longitude FLOAT,
    passenger_count INTEGER,
    trip_distance FLOAT,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type INTEGER,
    payment_type_name VARCHAR AS (
        CASE
            WHEN payment_type = 1 THEN 'Credit card'
            WHEN payment_type = 2 THEN 'Cash' 
            WHEN payment_type = 3 THEN 'No charge'
            WHEN payment_type = 4 THEN 'Dispute'
            WHEN payment_type = 5 THEN 'Unknown'
            WHEN payment_type = 6 THEN 'Voided trip'
            ELSE TO_VARCHAR(payment_type)  
        END
    ),
    tip_amount FLOAT,
    total_amount FLOAT,
    tolls_amount FLOAT,
    end_longitude FLOAT,
    end_latitude FLOAT,
    extra FLOAT,
    fare_amount FLOAT,
    improvement_surcharge FLOAT,
    mta_tax FLOAT,
    puYear INTEGER,
    puMonth INTEGER,
    RatecodeID INTEGER,
    RatecodeID_name VARCHAR AS (
        CASE
            WHEN RatecodeID = 1 THEN 'Standard rate'
            WHEN RatecodeID = 2 THEN 'JFK' 
            WHEN payment_type = 3 THEN 'Newark'
            WHEN payment_type = 4 THEN 'Nassau or Westchester'
            WHEN payment_type = 5 THEN 'Negotiated fare'
            WHEN payment_type = 6 THEN 'Group ride'
            ELSE TO_VARCHAR(RatecodeID)  
        END
    ),
    store_and_fwd_flag STRING,
    
    store_and_fwd_flag_exp STRING AS (
        CASE
            WHEN store_and_fwd_flag = 'Y' THEN 'store and forward trip'
            WHEN store_and_fwd_flag = 'N' THEN 'not a store and forward trip'
            ELSE 'Unknown'
        END
    ),
    ride_id VARCHAR AS (MD5(TO_VARCHAR(VendorID) || TO_VARCHAR(pickup_datetime) || TO_VARCHAR(dropoff_datetime) ))
) CLUSTER BY (pickup_datetime);

--Create Task to transform data from raw to processed schema 
CREATE OR REPLACE TASK transform_yellow_taxi_task
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
INSERT INTO nyc_taxi_lim.processed.yellow_taxi (
    VendorID ,
    VendorID_String,
    pickup_datetime,
    dropoff_datetime,
    start_latitude,
    start_longitude,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    tip_amount,
    total_amount,
    tolls_amount,
    end_longitude,
    end_latitude,
    extra,
    fare_amount,
    improvement_surcharge,
    mta_tax,
    puYear,
    puMonth,
    RatecodeID,
    store_and_fwd_flag
)
SELECT 
    VendorID,
    VendorID_String,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    startLat,
    startLon,
    passenger_count,
    trip_distance,
    PULocationID,
    DOLocationID,
    payment_type,
    tip_amount,
    total_amount,
    tolls_amount,
    endLon,
    endLat,
    extra,
    fare_amount,
    improvement_surcharge,
    mta_tax,
    puYear,
    puMonth,
    RatecodeID,
    STORE_AND_FWD_FLAG

FROM nyc_taxi_lim.raw.yellow_taxi_landing
WHERE tpep_pickup_datetime > (
    SELECT COALESCE(MAX(pickup_datetime), '1970-01-01'::TIMESTAMP_NTZ) 
    FROM nyc_taxi_lim.processed.yellow_taxi
);


EXECUTE TASK transform_yellow_taxi_task;
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('nyc_yellow_taxi.RAW.transform_yellow_taxi_task');
