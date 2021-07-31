from urllib import request

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="downloading_using_pythonOperator",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval = "@hourly",
)


def _get_data(execution_date):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path="/tmp/wikipageviews.gz"
    request.urlretrieve(url,output_path)
    

get_data = PythonOperator(
    task_id = "get_data",
    python_callable = _get_data,   #on execution, the pythonOperator executes the provided callable, which could be any function. Since it is a function, and not a string as with all other operators, the code within the function cannot be automatically templated. Instead, the task context variables can be provided and used in the given function.
    dag=dag,

)
 
