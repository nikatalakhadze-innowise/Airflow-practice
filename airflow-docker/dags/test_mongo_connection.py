from airflow import DAG
from airflow.sdk import task
from pendulum import datetime
from airflow.providers.mongo.hooks.mongo import MongoHook

with DAG(
    dag_id="test_mongo_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):

    @task
    def test():
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        # ping MongoDB
        result = client.admin.command("ping")
        print(result)

    test()