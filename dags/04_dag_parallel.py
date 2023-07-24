import time
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="04_dag",
    start_date=datetime.datetime.now(),
    schedule_interval="@once",
)

def task1_callable():
    time.sleep(5)
    print("Hello World!")

def task2_callable():
    time.sleep(4)
    print("Good Bye!")

task1 = PythonOperator(
    task_id="task1",
    python_callable=task1_callable,
    dag=dag,
)

task2 = PythonOperator(
    task_id="task2",
    python_callable=task2_callable,
    dag=dag,
)
