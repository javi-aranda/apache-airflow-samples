import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG("02_dag", start_date=datetime.datetime.now())

task1 = PythonOperator(
    task_id="task1",
    python_callable=lambda: (_ for _ in ()).throw(Exception('Task 1 failed, so Task 2 would not run')),
    dag=dag
)

task2 = PythonOperator(
    task_id="task2",
    python_callable=lambda: print("Hello from task2"),
    dag=dag,
    depends_on_past=True
)

task1 >> task2
