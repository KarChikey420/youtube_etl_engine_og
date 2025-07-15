from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract_og.youtube_api import get_channel_videos, get_video_stats
from transform.youtube import suggest_video_ideas
from load.loader import insert_video_stats
import pandas as pd

def extract_func(ti, **kwargs):
    # Get channel_id from dag_run.conf, fallback to default if not provided
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf.get('channel_id'):
        channel_id = dag_run.conf.get('channel_id')
    else:
        channel_id = 'UC_x5XG1OV2P6uZZ5FSM9Ttw'  # Default channel
    video_ids = get_channel_videos(channel_id, max_results=20)
    stats = get_video_stats(video_ids)
    return stats

def transform_func(ti, **kwargs):
    stats = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame(stats)
    suggestions = suggest_video_ideas(df)
    ti.xcom_push(key='df', value=df.to_dict())
    ti.xcom_push(key='suggestions', value=suggestions)
    return stats

def load_func(ti, **kwargs):
    stats = ti.xcom_pull(task_ids='transform')
    insert_video_stats(stats)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'youtube_etl_pipeline',
    default_args=default_args,
    description='A YouTube ETL pipeline using PythonOperator for every task, with dynamic channel_id',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_func,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_func,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_func,
        provide_context=True,
    )

    extract >> transform >> load
