# markazapp
Markaz App Assessment

![Screenshot](images/datapipeline_airflow.png)

## Task 1 - using Apache Airflow

### Tool stack used:
- AWS S3
- Apache Airflow via Astro
- Snowflake Cloud Datawarehouse

### Files in this task

- s3_file_upload - DAG.py -> Airflow DAG that moves data from local storage to aws s3 bucket
- transaction_load - DAG.py -> Airflow DAG that ingests data from s3 bucket, applies transformations on the datasets, and loads the resultant dataset to Snowflake table. Following steps are included in the DAG
    - 
