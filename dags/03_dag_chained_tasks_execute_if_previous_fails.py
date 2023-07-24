import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


dag = DAG("03_dag", start_date=datetime.datetime.now())

def task1_callable():
    raise Exception("Task 1 failed, but task 2 could run")

task1 = PythonOperator(
    task_id="task1",
    python_callable=task1_callable,
    dag=dag,
)

task2 = PythonOperator(
    task_id="task2",
    python_callable=lambda: print("Hello from task2"),
    dag=dag,
    depends_on_past=False,
    trigger_rule=TriggerRule.ALL_FAILED,
)

task1 >> task2
