from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from extract.albums.module import pandas_sqlite_csv
from datetime import datetime


args = {
    'owner': 'Serginho',
    'start_date': days_ago(1) # make start date in the past
}

#defining the dag object
dag = DAG(
    dag_id='extract-albums',
    default_args=args,
    schedule_interval=None #to make this workflow happen every day
)

#todo path env
with dag:
    step1 = BashOperator(
                task_id='step1', 
                bash_command='source /home/sergio/dev/python/music-analysis-pipeline/.env/bin/activate && python /home/sergio/dev/python/music-analysis-pipeline/airflow/dags/extract/albums/module.py',
            )
