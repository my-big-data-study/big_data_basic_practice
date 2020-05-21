from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['mhtang@thoughtworks.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'summer_project',
    default_args=default_args,
    description='summer project',
    schedule_interval=timedelta(minutes=5),
)

start_pipeline = BashOperator(
    task_id='start_pipeline',
    bash_command='date',
    dag=dag,
)

save_full_data_job = BashOperator(
    task_id='save_full_data_job',
    bash_command='spark-submit --master yarn s3://mhtang/summer_project/save_full_data.py --source http://data.gdeltproject.org/gdeltv2/masterfilelist.txt --sink s3://mhtang/summer_project/output',
    dag=dag
)

spark_submit_job = BashOperator(
    task_id='spark_submit_job',
    bash_command='spark-submit --master yarn s3://mhtang/data-practice/aws_spark_job.py --source s3://mhtang/data-practice/t8.shakespeare.txt --sink s3://mhtang/data-practice/output',
    dag=dag
)

start_pipeline >> save_full_data_job >> spark_submit_job
