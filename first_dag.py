from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
 
 
# Step 1: a normal Python function
def say_hello():
    print("Hello from Airflow! This DAG came from GitHub.")
 
 
# Step 2: create the pipeline and add the function as a task
with DAG(
    dag_id="hello_github",           # name shown in Airflow
    schedule=None,                   # runs only when you click Trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,                   # do not run for past dates
):
    hello = PythonOperator(
        task_id="say_hello",         # name of this step
        python_callable=say_hello,   # the function to run
    )