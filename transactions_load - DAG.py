import os
from datetime import datetime, date
from airflow.models import DAG
from pandas import DataFrame 

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

print(os.getcwd())

current_date = str(date.today())

S3_FILE_PATH = 's3://markazapp-data/markazapp-raw/' + current_date
S3_CONN = 'aws_conn'
SF_LOAD_CONN = 'snowflake_load_conn'
SF_STAGE_CONN = 'snowflake_stage_conn'



@aql.transform
def filter_invalid_data(input_table: Table):
    return '''
        SELECT DISTINCT 
            customer_id,
            product_id,
            purchase_date,
            quantity,
            product_price,
            total_price,
            rating,
            review
        FROM {{input_table}} 
        WHERE product_id IS NOT NULL
      '''

@aql.transform
def derive_product_category(input_table: Table):
    return '''
        SELECT customer_id,
            product_id,
            purchase_date,
            quantity,
            product_price,
            total_price,
            rating,
            review,
            case 
                when product_id = 1 then 'Mobile phones'
                when product_id = 2 then 'Game consoles'
                when product_id = 3 then 'Household furniture'
                when product_id = 4 then 'Home appliances'
                when product_id = 5 then 'Clothing'
                when product_id = 6 then 'Snow Sports'
                when product_id = 7 then 'Camping & Hiking'
                when product_id = 8 then 'Yard Games'
                when product_id = 9 then 'Make-up'
                when product_id = 10 then 'Beauty'
                else 'Others'
            end as product_category
        FROM {{input_table}}'''

@aql.transform
def add_tombstone_fields(input_table: Table):
    return '''
        SELECT customer_id,
            product_id,
            purchase_date,
            quantity,
            product_price,
            total_price,
            rating,
            review,
            product_category,
            current_timestamp as updated_at
        FROM {{input_table}}
    '''

@aql.transform
def adjust_datetime_fields(input_table: Table):
    return '''
        SELECT customer_id,
            product_id,
            TO_DATE(purchase_date, 'MM-DD-YYYY') as purchase_date,
            quantity,
            product_price,
            total_price,
            rating,
            review,
            product_category,
            updated_at
        FROM {{input_table}}
    '''

@aql.transform
def product_sales_per_date(input_table: Table):
    return '''
        SELECT product_category,
        purchase_date,
        sum(total_price) as total_sales
        FROM {{input_table}}
        GROUP BY 1,2
    '''

@aql.transform
def review_ratings(input_table: Table):
    return '''
        SELECT product_category, 
            count(product_id) as total_orders,
            count(review) as total_reviews,
            count(case when review ilike '%good%' then 'Positive'end) as total_positive_reviews,
            count(case when review ilike '%bad%' then 'Positive'end) as total_negative_reviews
        FROM markazapp.stage.transactions
        GROUP BY 1
    '''

@aql.transform
def avg_ratings_per_product(input_table: Table):
    return '''
        SELECT product_category, 
                purchase_date, 
                rating,
                avg(rating) over(partition by product_category order by purchase_date) rolling_avg_rating
        FROM transactions
        ORDER BY purchase_date;
    '''

@aql.run_raw_sql
def truncate_reporting_table(table: Table):
    """Truncate reporting table"""
    return """
      TRUNCATE TABLE {{table}}
    """

with DAG(dag_id='MarkazApp_ETL', start_date=datetime(2023,10,17), schedule='@daily', catchup=False):
    
    transactions_table_file_load = aql.load_file(
        input_file=File(
            path=S3_FILE_PATH + '/transactions.csv',conn_id = S3_CONN
        ),
        output_table = Table(conn_id = SF_LOAD_CONN)

    )

    load_data = filter_invalid_data(transactions_table_file_load)

    product_category_column_added = derive_product_category(load_data)

    datetime_columns_added = add_tombstone_fields(product_category_column_added)

    datetime_fields_adjusted = adjust_datetime_fields(datetime_columns_added)

    staged_data = aql.merge(
        task_id='staged_data_task',
        target_table=Table(
            name='transactions',
            conn_id = SF_LOAD_CONN
        ),
        source_table = datetime_fields_adjusted,
        target_conflict_columns=["customer_id"],
        columns=["customer_id", "product_id", "purchase_date" ,"quantity", "product_price",
                  "total_price" ,"rating" ,"review","product_category", "updated_at"],
        if_conflicts ="update",
    )

    reporting_sales_per_day_data = product_sales_per_date(staged_data)
    reporting_table = Table(name="sales_per_product", conn_id=SF_LOAD_CONN) 
    truncate_sales_per_product = truncate_reporting_table(reporting_table)

    insert_sales_per_product = aql.append(
        task_id = 'sales_per_product_insert',
        source_table=reporting_sales_per_day_data,
        target_table=reporting_table,
    )

    review_ratings_per_product_category = review_ratings(staged_data)
    reporting_table = Table(name="review_ratings", conn_id=SF_LOAD_CONN) 
    truncate_review_ratings = truncate_reporting_table(reporting_table)

    insert_review_ratings = aql.append(
        task_id = 'review_ratings_insert',
        source_table=review_ratings_per_product_category,
        target_table=reporting_table,
    )

    avg_ratings = avg_ratings_per_product(staged_data)
    reporting_table = Table(name="rolling_avg_rating_per_product", conn_id=SF_LOAD_CONN) 
    truncate_rolling_avg_rating_per_product = truncate_reporting_table(reporting_table)

    insert_rolling_avg_rating_per_product = aql.append(
        task_id = 'rolling_avg_rating_per_product_insert',
        source_table=avg_ratings,
        target_table=reporting_table,
    )
    

#transactions_table_file_load >> load_data >> product_category_column_added >> datetime_columns_added >> datetime_fields_adjusted >> staged_data >> reporting_sales_per_day_data >> truncate_table >> record_results >> aql.cleanup()
staged_data >> reporting_sales_per_day_data >> truncate_sales_per_product >> insert_sales_per_product >> review_ratings_per_product_category >> truncate_review_ratings >> insert_review_ratings >> avg_ratings >> truncate_rolling_avg_rating_per_product >> insert_rolling_avg_rating_per_product >> aql.cleanup()