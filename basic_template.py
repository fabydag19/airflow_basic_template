from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG

#Set common parameters of DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

#DAG Configuration
dag = DAG(
    dag_id = 'example_dag',
    default_args = default_args,       
    schedule_interval = '@hourly', 
    catchup = False, #If catchup is False, Airflow skip previous execution (from start_date to now)
    tags = ['example'],
)

#Script execution function
def open_script():
    script_path = 'SCRIPT PATH'
    with open (script_path, 'r') as file: #Read script
        exec(file.read())                 #Execute script

#DAG execution
PythonOperator(
    task_id = 'example_task',
    python_callable = open_script,
    dag = dag,
)