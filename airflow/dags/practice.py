from datetime import timedelta

import requests
from pyspark import SparkConf, SparkContext

from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
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
    'airflow_practice',
    default_args=default_args,
    description='airflow practice for first task',
    schedule_interval=timedelta(hours=1),
)


def fail_pipeline_with_msg():
    print("pipeline failed")


def pipeline_complete_with_msg():
    print("pipeline succeed")


source = "https://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt"


def validate_source_check():
    status_code = requests.get(source).status_code
    if status_code == 200:
        return "succeed_check_source_availability"
    else:
        return "fail_check_source_availability"


def read_data():
    spark_conf = SparkConf().setAppName("airflow_practice").setMaster("spark://6377f818db72:7077")
    spark_context = SparkContext(spark_conf)
    words = spark_context.textFile(source).flatMap(lambda row: row.split(" "))
    word_count = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    word_count.saveAsTextFile("output.txt")


def validate_process_result():
    try:
        read_data()
        return "pipeline_succeed"
    except:
        return "pipeline_failed"


check_source_availability = BranchPythonOperator(task_id="check_source_availability",
                                                 python_callable=validate_source_check,
                                                 dag=dag)

succeed_check_source_availability = BranchPythonOperator(task_id="succeed_check_source_availability",
                                                         python_callable=validate_process_result,
                                                         dag=dag)

fail_check_source_availability = PythonOperator(task_id="fail_check_source_availability",
                                                python_callable=fail_pipeline_with_msg,
                                                dag=dag)

pipeline_succeed = PythonOperator(task_id="pipeline_succeed",
                                  python_callable=pipeline_complete_with_msg,
                                  dag=dag)

pipeline_failed = PythonOperator(task_id="pipeline_failed",
                                 python_callable=fail_pipeline_with_msg,
                                 dag=dag)

check_source_availability >> succeed_check_source_availability >> [pipeline_succeed, pipeline_failed]
check_source_availability >> fail_check_source_availability
