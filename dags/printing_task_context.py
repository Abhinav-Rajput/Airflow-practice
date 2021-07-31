import airflow.utils.dates
from airflow import DAG
import logging
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="printing_context",
    start_date = airflow.utils.dates.days_ago(3),
    schedule_interval = "@daily",

)


def _print_context(**context):        ## this argument indicates we expect Airflow task context. Context varaible is a dict of all context variables.
    print(context)
    start = context["execution_date"]
    end   = context["next_execution_date"]
    print(f"Start: {start}, end: {end}")
    

print_context = PythonOperator(
    task_id='print_context',
    python_callable = _print_context,
    dag = dag,
)