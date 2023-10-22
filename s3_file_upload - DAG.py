from datetime import datetime, timedelta, date, timezone
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook


current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_conn')

    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)

with DAG(dag_id='s3_file_upload', start_date=datetime(2023,10,18), schedule='@daily', catchup=False):

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/usr/local/airflow/data/transactions.csv',
            'key': 'markazapp-raw/'+current_date+'/transactions.csv',
            'bucket_name': 'markazapp-data'
        }
    )

task_upload_to_s3
                 