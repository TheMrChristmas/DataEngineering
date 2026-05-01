from src.writer import Writer
from src.transformer import Transformer
from src.validator import Validator
from src.reader import Reader
from datetime import datetime
from pathlib import Path
import sys

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parent))


def ingest(**context):
    path = Reader().read("2026-01")
    return path


def validate(**context):
    path = context["ti"].xcom_pull(task_ids="ingest")
    Validator().validate(path)


def transform(**context):
    path = context["ti"].xcom_pull(task_ids="ingest")
    output_path = Transformer().process(path)
    return output_path


def load(**context):
    path = context["ti"].xcom_pull(
        task_ids="transform")
    Writer().write(path)


with DAG(
    dag_id="yellow_taxi_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 1,4 * *",
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="ingest",    python_callable=ingest)
    t2 = PythonOperator(task_id="validate",  python_callable=validate)
    t3 = PythonOperator(task_id="transform", python_callable=transform)
    t4 = PythonOperator(task_id="load",      python_callable=load)

    t1 >> t2 >> t3 >> t4
