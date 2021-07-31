import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

##### objective of this script is  to consume data from an API, and perform some calculation
### As it is evident from above objective its a 2 step process, consume & calculate
### we would be creating two tasks tied to following 1 DAG

dag = DAG(
    dag_id="01_unscheduled",
    schedule_interval= "@daily", # this specify that this is an unscheduled dag
    start_date = dt.datetime(year=2021, month=1, day=3),  # this defines the start date for the dag
    end_date=dt.datetime(year=2021, month=1, day=5),
                   
)


# defining the  1st Task using BashOperator, to make directory , and load the data
fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command = (
        "mkdir -p /home/ubuntu/airflow/data && "
        "curl -o /home/ubuntu/airflow/data/events.json"
        "http://0.0.0.0:5000/events?"
        "start_date={{execution_date.strftime('%Y-%m-%d')}}"
        "&end_date={{next_execution_date.strftime('%Y-%m-$d')}}"           ## formated execution_date inserted with jinja templating, next_execution_date holds the execution date of the next interval ## here we can also use any REST-API, to make things simple, and have light data, I have created a flask based local API
    ),
    dag= dag,
)


## following function is defined for calculating the stats, and to be used in python operator task
## we can also write following function in separate Python file and imp
def _calculate_stats(input_path, output_path):
    """ Calculate event statistics."""
    
    events = pd.read_json(input_path)                           #loading the events data
    stats = events.groupby(["date","user"]).size().reset_index()# calculating the required statistics
    Path(output_path).parent.mkdir(exist_ok=True)  #exist_ok=True this is to make sure the directory exist
    stats.to_csv(output_path, index=False)   # finally eriting the calculated stats on .csv files
    



## following is 2nd Taks    
calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/home/ubuntu/airflow/data/events.json",
        "output_path":"/home/ubuntu/airflow/data/stats.csv",
    },
    dag=dag,             #  this tells given task, that it is tied to which DAG
)
 

##setting order of execution
fetch_events  >> calculate_stats