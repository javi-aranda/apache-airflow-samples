import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG("01_dag", start_date=datetime.datetime.now())

task1 = PythonOperator(
    task_id="task1",
    python_callable=lambda: print("Hello from task1"),
    dag=dag
)

task2 = PythonOperator(
    task_id="task2",
    python_callable=lambda: print("Hello from task2"),
    dag=dag
)

task1 >> task2