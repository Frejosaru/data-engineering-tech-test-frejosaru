from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor

from src.config import (
    CSV_PATH, SQLALCHEMY_URL, TABLE_NAME, CHUNKSIZE, MIN_FILE_SIZE_BYTES
)
from src.common.logger import logger
from src.etl.extract import extract_by_chunks, file_size_ok
from src.etl.transform import basic_transform, count_rows
from src.etl.load import load_chunk, validate_non_empty
from src.common.exceptions import ValidationError

default_args = {
    "owner": "frejosaru",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def _check_min_size(**_):
    return file_size_ok(CSV_PATH, MIN_FILE_SIZE_BYTES)

def _run_etl(**context):
    total_rows = 0
    first = True
    for chunk in extract_by_chunks(CSV_PATH, CHUNKSIZE):
        t_chunk = basic_transform(chunk)
        rows = count_rows(t_chunk)
        total_rows += rows
        load_chunk(t_chunk, SQLALCHEMY_URL, TABLE_NAME, if_exists="replace" if first else "append")
        first = False
        logger.info(f"Processed chunk rows={rows} total_rows={total_rows}")

    context["ti"].xcom_push(key="total_rows", value=total_rows)

def _validate_and_alert(**context):
    try:
        rows = validate_non_empty(SQLALCHEMY_URL, TABLE_NAME)
        logger.info(f"Validation OK. {rows} rows in dest.")
    except ValidationError as e:
        logger.error(f"[ALERT] Validation failed: {e}")
        raise

with DAG(
    dag_id="etl_transactions_local",
    description="ETL local de 1M filas con sensores, retries y validaciones",
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["tech-test", "frejosaru"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=CSV_PATH,
        poke_interval=15,
        timeout=60 * 30,
        mode="poke",
    )

    wait_for_min_size = PythonSensor(
        task_id="wait_for_min_size",
        python_callable=_check_min_size,
        poke_interval=15,
        timeout=60 * 30,
        mode="poke",
    )

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=_run_etl,
        provide_context=True,
    )

    validate_and_alert = PythonOperator(
        task_id="validate_and_alert",
        python_callable=_validate_and_alert,
        provide_context=True,
        trigger_rule="all_done",
    )

    wait_for_file >> wait_for_min_size >> run_etl >> validate_and_alert
