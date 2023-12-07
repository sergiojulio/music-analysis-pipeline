from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'Serginho',
    'start_date': days_ago(1)
}

# defining the dag object
dag = DAG(
    dag_id='extract-r_albums_artists',
    default_args=args,
    schedule_interval=None
)

with dag:
    step1 = BashOperator(
                task_id='step1', 
                bash_command='source /home/sergio/dev/python/music-analysis-pipeline/.env/bin/activate && python '
                             '/home/sergio/dev/python/music-analysis-pipeline/src/r_albums_artists/pandas/r_albums_artists.py',
            )
    
    step2 = BashOperator(
                task_id='step2', 
                bash_command="""
                                export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64/";
                                export PATH=$PATH:$JAVA_HOME/bin;
                                spark-submit /home/sergio/dev/python/music-analysis-pipeline/src/r_albums_artists/pyspark/r_albums_artists.py
                            """,
            )

step1 >> step2
