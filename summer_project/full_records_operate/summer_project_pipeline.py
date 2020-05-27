from datetime import timedelta

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from airflow import DAG

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

# save_full_data_job = BashOperator(
#     task_id='save_full_data_job',
#     bash_command='spark-submit --master yarn s3://mhtang/summer_project/save_full_data.py --source http://data.gdeltproject.org/gdeltv2/masterfilelist.txt --sink s3://mhtang/summer_project/output/master_file_list.parquet',
#     dag=dag
# )

# save_data_to_aws_job = BashOperator(
#     task_id='save_data_to_aws_job',
#     bash_command="spark-submit --master yarn s3://mhtang/summer_project/save_data_to_aws.py --sink s3://mhtang/summer_project/source/master_file_list.parquet.gzip",
#     dag=dag
# )

operate_data_from_aws_job = BashOperator(
    task_id='operate_data_from_aws_job',
    bash_command="spark-submit --packages org.elasticsearch:elasticsearch-spark-20_2.11:7.4.2 --master yarn s3://mhtang/summer_project/operate_data_from_aws.py  --source s3://mhtang/summer_project/source/*.CSV",
    dag=dag
)

start_pipeline >> operate_data_from_aws_job
