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
    'retry_delay': timedelta(seconds=5),
}
dag = DAG(
    'summer_project',
    default_args=default_args,
    description='summer project',
    schedule_interval=timedelta(hours=1),
)

# spark_submit_operator = SparkSubmitOperator(task_id='spark_submit_job',
#                                             conn_id="spark_default",
#                                             application="s3://mhtang/data-practice/aws_spark_job.py",
#                                             dag=dag)
#
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t3 = BashOperator(
    task_id='spark_submit_job',
    bash_command='spark-submit --master yarn s3://mhtang/data-practice/aws_spark_job.py --source s3://mhtang/data-practice/t8.shakespeare.txt --sink s3://mhtang/data-practice/output',
    dag=dag
)

t1 >> t3
