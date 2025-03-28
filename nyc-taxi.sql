--SQL codes to ingest data from Azure Blob to SNowflake

-- Create the database
CREATE DATABASE nyc_yellow_taxi 

-- Create the schema
CREATE SCHEMA raw

-- Create another schema
CREATE SCHEMA processed

--Cretae warehouse
CREATE WAREHOUSE nyc_yellow_taxi_wh with warehouse_size = 'XSMALL' 

--Create external Stage
-- The stage to read files from Blob
CREATE OR REPLACE STAGE nyc_taxi_stage
  URL = 'azure://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/'
  FILE_FORMAT = (TYPE = PARQUET);

-- In Raw schema, create the raw table
CREATE OR REPLACE TABLE raw_taxi_trips (
  DOLocationID INTEGER,
  endLon FLOAT,
  endLat FLOAT,
  extra FLOAT,
  fare_amount FLOAT,
  improvement_surcharge FLOAT,
  mta_tax FLOAT,
  passenger_count INTEGER,
  payment_type INTEGER,
  PULocationID INTEGER,
  puYear INTEGER,
  puMonth INTEGER,
  RatecodeID INTEGER,
  startLon FLOAT,
  startLat FLOAT,
  store_and_fwd_flag STRING,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  total_amount FLOAT,
  tpep_dropoff_datetime TIMESTAMP_NTZ,
  tpep_pickup_datetime TIMESTAMP_NTZ,
  trip_distance FLOAT,
  VendorID_String VARCHAR(50),
  VendorID INTEGER,
  file_name VARCHAR,
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- In  Raw schema, create the stream
CREATE OR REPLACE STREAM raw_taxi_trips_stream 
ON TABLE nyc_taxi_raw;

-- In Raw schema, create the task to ingest data from Azure Blob to Snowflake
CREATE OR REPLACE TASK ingest_taxi_trips

-- In raw schema, create a raw table to ingest data partially there
CREATE OR REPLACE TABLE nyc_taxi_raw (
    data VARIANT
);

CREATE OR REPLACE PIPE nyc_taxi_raw_pipe
AUTO_INGEST = TRUE
AS
COPY INTO nyc_taxi_raw
FROM @nyc_taxi_stage
PATTERN = '*.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE'

--Ingest from raw format to table
CREATE OR REPLACE TASK process_taxi_raw_to_trip
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 0 0 * * 5 UTC' 
AS
INSERT INTO raw_taxi_trips (
  DOLocationID,
  endLon,
  endLat,
  extra,
  fare_amount,
  improvement_surcharge,
  mta_tax,
  passenger_count,
  payment_type,
  PULocationID,
  puYear,
  puMonth,
  RatecodeID,
  startLon,
  startLat,
  store_and_fwd_flag,
  tip_amount,
  tolls_amount,
  total_amount,
  tpep_dropoff_datetime,
  tpep_pickup_datetime,
  trip_distance,
  VendorID_String,
  VendorID,
  load_timestamp
)
SELECT
  data:"doLocationId"::INTEGER AS DOLocationID,
  NULL AS endLon, 
  NULL AS endLat, 
  data:"extra"::FLOAT AS extra,
  data:"fareAmount"::FLOAT AS fare_amount,
  data:"improvementSurcharge"::FLOAT AS improvement_surcharge,
  data:"mtaTax"::FLOAT AS mta_tax,
  data:"passengerCount"::INTEGER AS passenger_count,
  data:"paymentType"::INTEGER AS payment_type,
  data:"puLocationId"::INTEGER AS PULocationID,
  EXTRACT(YEAR, TO_TIMESTAMP(data:"tpepPickupDateTime"::STRING)) AS puYear,
  EXTRACT(MONTH, TO_TIMESTAMP(data:"tpepPickupDateTime"::STRING)) AS puMonth,
  data:"rateCodeId"::INTEGER AS RatecodeID,
  NULL AS startLon, 
  NULL AS startLat, 
  data:"storeAndFwdFlag"::STRING AS store_and_fwd_flag,
  data:"tipAmount"::FLOAT AS tip_amount,
  data:"tollsAmount"::FLOAT AS tolls_amount,
  data:"totalAmount"::FLOAT AS total_amount,
  TO_TIMESTAMP(data:"tpepDropoffDateTime"::STRING) AS tpep_dropoff_datetime,
  TO_TIMESTAMP(data:"tpepPickupDateTime"::STRING) AS tpep_pickup_datetime,
  data:"tripDistance"::FLOAT AS trip_distance,
  data:"vendorID"::VARCHAR(50) AS VendorID_String,
  data:"vendorID"::INTEGER AS VendorID,
  
  CURRENT_TIMESTAMP() AS load_timestamp
FROM nyc_taxi_raw
WHERE data:"vendorID" RLIKE '^[12]$'; 

EXECUTE TASK process_taxi_raw_to_trip;
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('nyc_yellow_taxi.RAW.process_taxi_raw_to_trip');

