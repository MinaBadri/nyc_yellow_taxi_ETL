--dbt sql
-- models/fact_taxi_trips.sql
{{ config(
    materialized='incremental',
    unique_key='ride_id'
) }}

WITH transformed_data AS (
    SELECT 
        
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

        DATEDIFF('minute', CAST(tpep_pickup_datetime AS TIMESTAMP), CAST(tpep_dropoff_datetime AS TIMESTAMP)) AS trip_duration_minutes,

        EXTRACT(HOUR FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS hour_of_day,
        EXTRACT(DAYOFWEEK FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS day_of_week,

        {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']) }} AS ride_id,

        CASE
            WHEN RatecodeID = 1 THEN 'Standard rate'
            WHEN RatecodeID = 2 THEN 'JFK' 
            WHEN RatecodeID = 3 THEN 'Newark'
            WHEN RatecodeID = 4 THEN 'Nassau or Westchester'
            WHEN RatecodeID = 5 THEN 'Negotiated fare'
            WHEN RatecodeID = 6 THEN 'Group ride'
            ELSE TO_VARCHAR(RatecodeID)  
        END AS RatecodeID_name,

        CASE
            WHEN payment_type = 1 THEN 'Credit card'
            WHEN payment_type = 2 THEN 'Cash'
            WHEN payment_type = 3 THEN 'No charge'
            WHEN payment_type = 4 THEN 'Dispute'
            WHEN payment_type = 5 THEN 'Unknown'
            WHEN payment_type = 6 THEN 'Voided trip'
            ELSE 'Other'
        END AS payment_type_name,

        CASE 
            WHEN VendorID = 1 THEN 'Creative Mobile Technologies' 
            WHEN VendorID = 2 THEN 'VeriFone Inc' 
            ELSE VendorID_String 
        END AS Vendor_name,

        CASE 
            WHEN store_and_fwd_flag = 'Y' THEN 'store and forward trip'
            WHEN store_and_fwd_flag = 'N' THEN 'not a store and forward trip'
            ELSE 'Unknown'
        END AS store_and_fwd_status,

        DOLocationID AS dropoff_location_id,
        endLon AS end_longitude,
        endLat AS end_latitude,
        extra,
        fare_amount,
        improvement_surcharge,
        mta_tax,
        passenger_count,
        payment_type,
        PULocationID AS pickup_location_id,
        puYear,
        puMonth,
        RatecodeID,
        startLon AS start_longitude,
        startLat AS start_latitude,
        store_and_fwd_flag ,
        tip_amount,
        tolls_amount,
        total_amount,
        trip_distance,
        VendorID_String,
        VendorID,
        load_timestamp
    

    FROM 
        {{source('raw','RAW_TAXI_TRIPS') }}
)

SELECT *
    -- ride_id,  -- The unique ride ID
    -- pickup_datetime,
    -- dropoff_datetime,
    -- trip_duration_minutes,
    -- hour_of_day,
    -- day_of_week, 
    
FROM 
    transformed_data


{% if is_incremental() %}
    WHERE tpep_pickup_datetime > (SELECT MAX(tpep_pickup_datetime) FROM {{ this }})
{% endif %}
