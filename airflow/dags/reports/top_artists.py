from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'Serginho',
    'start_date': days_ago(1)
}

# defining the dag object
dag = DAG(
    dag_id='top-artists',
    default_args=args,
    schedule_interval=None
)

with dag:
    step1 = BashOperator(
                task_id='step1', 
                bash_command='source /home/sergio/dev/python/music-analysis-pipeline/.env/bin/activate && cd /home/sergio/dev/python/music-analysis-pipeline/music_analysis_pipeline && '
                             'dbt run --model top_artists_model',
            )
    

step1
