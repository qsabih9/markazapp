# Markaz APP ETL tasks
The README is describing the steps/approaches taken towards the Data Engineering task

## Task 1 - using Apache Airflow

Below screenshot is the architecture followed in moving the data from csv file all the way to Snowflake
![Screenshot](images/datapipeline_airflow.png)

### Tech stack used:
- AWS S3
- Apache Airflow via Astro
- Snowflake Cloud Datawarehouse

### Files in this task

- s3_file_upload - DAG.py -> Airflow DAG that moves data from local storage to aws s3 bucket
- transaction_load - DAG.py -> Airflow DAG that ingests data from s3 bucket, applies transformations on the datasets, and loads the resultant dataset to Snowflake table

Following steps are included in transaction_load DAG:

- load_file : This loads the file from s3 to dataset in python
- filter_invalid_data : This removes all the rows that have product_id as NULL and also removes complete row duplicates 
- derive_product_category : This generates a product_category column and appends in dataset based on product_id value
- add_tombstone_fields : This adds updated_time value in the dataset which is equal to current_timestamp
- adjust_datetime_fields : This formats the purchase_date column values so it can be casted into date column
- staged_data_task : This moves the data from the datasets into snowflake transactions table. Its a merge step based on customer_id
- product_sales_per_date : This creates a dataset for the products sales for each date
- truncate_reporting_table : This truncates sales_per_product table in snowflake
- sales_per_product_insert : This inserts products_sales_per_date dataset into sales_per_product table in snowflake
- review_ratings : This creates a dataset for total positive and negative review counts for each product_category
- truncate_reporting_table : This truncates review_ratings table in snowflake
- review_ratings_insert : This inserts review_ratings dataset into review_ratings table in snowflake
- avg_ratings_per_product : This creates a dataset for the average ratings for each product date wise. Its a rolling average
- truncate_reporting_table : This truncates rolling_avg_rating_per_product table in snowflake
- rolling_avg_rating_per_product_insert : This inserts avg_ratings_per_product dataset into rolling_avg_rating_per_product table in snowflake
- cleanup : This deletes all the temporary tables created along the way
  
