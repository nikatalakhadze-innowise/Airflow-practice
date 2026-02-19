from airflow.sdk import task, dag, Asset
from pendulum import datetime
import os
import pandas as pd
from airflow.providers.mongo.hooks.mongo import MongoHook

INTERMEDIATE_FILEPATH = '/opt/airflow/data/modified.csv'
INTERMEDIATE_FILE = Asset(INTERMEDIATE_FILEPATH)

@dag(
    dag_id='mongo_loader',
    start_date=datetime(2026, 1, 1),
    description='recieves data modified by data_loader, loads it into mongodb, deletes modified file',
    schedule=[INTERMEDIATE_FILE],
    catchup=False
)
def mongo_loader():

    @task
    def load_into_mongo():
        df = pd.read_csv(INTERMEDIATE_FILEPATH)
        hook = MongoHook(mongo_conn_id='mongo_default')
        
        records = df.to_dict(orient='records')
        hook.insert_many(
            mongo_db='Data',
            mongo_collection='modified-data',
            docs=records,
            ordered=True
        )

    @task
    def delete_leftover():
        os.remove(INTERMEDIATE_FILEPATH)
    
    load_into_mongo() >> delete_leftover()
    
mongo_loader()