from airflow.sdk import dag, task, Asset
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import TaskGroup
from pendulum import datetime
import regex
import pandas as pd
import os

FILEPATH = '/opt/airflow/data/tiktok_google_play_reviews.csv'
INTERMEDIATE_FILEPATH = '/opt/airflow/data/modified.csv'
INTERMEDIATE_FILE = Asset(INTERMEDIATE_FILEPATH)

@dag(
    dag_id='data_loader',
    start_date=datetime(2026, 1, 1),
    description='waits for a csv file in the data folder, loads it in, cleans it and saves it for mongo_loader',
    schedule=None,
    catchup=False,
)
def data_loader():
    
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=FILEPATH,
        poke_interval=10,
        timeout=5 * 60,
        mode='poke',
    )

    @task.branch
    def is_file_empty():
        if os.path.getsize(FILEPATH) == 0:
            return 'handle_empty_file'
        else:
            return 'process_data_group'
    
    @task.bash
    def handle_empty_file():
        return 'echo "file is empty"'
    
    with TaskGroup('process_data_group') as process_data_group:

        @task
        def load_data():
            df = pd.read_csv(FILEPATH)
            df.to_csv(INTERMEDIATE_FILEPATH, index=False)

        @task
        def sort_by_time():
            df = pd.read_csv(INTERMEDIATE_FILEPATH)
            df['at'] = pd.to_datetime(df['at'], format="%Y-%m-%d %H:%M:%S", errors='coerce')
            df = df.sort_values(by='at', ascending=True)
            df.to_csv(INTERMEDIATE_FILEPATH, index=False)
        
        @task
        def replace_nulls_with_dashes():
            df = pd.read_csv(INTERMEDIATE_FILEPATH)
            df.fillna('-', inplace=True)
            df.to_csv(INTERMEDIATE_FILEPATH, index=False)

        @task(outlets=[INTERMEDIATE_FILE])
        def clean_content():
            df = pd.read_csv(INTERMEDIATE_FILEPATH)
            pattern = regex.compile(r"[^\p{L}\p{N}\p{P}\s]+")
            df['content'] = df['content'].apply(lambda s: s if pd.isna(s) else pattern.sub('', str(s)))
            df.to_csv(INTERMEDIATE_FILEPATH, index=False)

        
        load_data() >> sort_by_time() >> replace_nulls_with_dashes() >> clean_content()
        

    wait_for_file >> is_file_empty() >> [handle_empty_file(), process_data_group]
        


data_loader()