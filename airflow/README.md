1. 进入airflow容器内部
2. 可以执行一下命令
~~~
airflow list_dags
airflow list_tasks airflow_practice --tree
airflow list_tasks tutorial --tree
airflow backfill tutorial -s 2020-05-15 -e 2020-06-07
airflow backfill airflow_practice -s 2020-05-15 -e 2020-06-07
~~~ 