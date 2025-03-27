#### New York City Yellow Taxi Trip Data Pipeline


Dataset: [yellow taxi trip records](https://learn.microsoft.com/en-us/azure/open-datasets/dataset-taxi-yellow?tabs=azureml-opendatasets)

Tech stack: Azure Blob Storage, Snowflake, dbt

The first stgae of pipeline is ingesting data from Azure Blob to Snowflake. One thing that needs to be considered is that the dataset is public, though it means no need to set-up roles and define access which makes working with Blob much easier, scheduling from Snowflake side is a little complicated. So, it means for this pipeline, there is nor need for an Azure acoount or changing any settings there.


For creating continuous ingestion, there is one thing to consider. It's not possible to create event notifications or form a queue in a public Blob dataset. So this part of the task can not be done by Snowpipe. 


So, the pipeline has been done with the assumption that our task is doing the datapipline automation from Snowflake. The SQL codes can be seen in  [Ingesting from Blob to Snowflake](https://github.com/MinaBadri/nyc_yellow_taxi_ETL/blob/main/nyc-taxi.sql). Further processes has been done on data and data has been copied to a final table. SQL codes for these process can be seen [in proessed sql file](https://github.com/MinaBadri/nyc_yellow_taxi_ETL/blob/main/processed.sql)


For dbt automation, I used this [tutorial](https://quickstarts.snowflake.com/guide/data_teams_with_dbt_cloud/#0) as a guiding hand. The [Video](https://www.youtube.com/watch?v=84RA7TuhCpg). This Tutorial is connecting to dbt via Partner Connect in Snowflake. However, if you already have the dbt account and made the connection, you can just use the set-ups. 

The only package I installed in dbt is as followed. I used the package for creating a primary key and to check if the content of one table is always positive. How these two can be done can be seen in [dbt sql] (https://github.com/MinaBadri/nyc_yellow_taxi_ETL/blob/main/dbtSQL.sql) and [yml for dbt](https://github.com/MinaBadri/nyc_yellow_taxi_ETL/blob/main/columns-test.yml) 

```
packages:
  - version: 1.3.0 #0.8.4
    package: dbt-labs/dbt_utils
```


The ETL Overview:


![Image Description](https://github.com/MinaBadri/nyc_yellow_taxi_ETL/blob/main/ETL.png)



