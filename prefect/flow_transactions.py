from prefect import flow, task
from src.config import CSV_PATH, SQLALCHEMY_URL, TABLE_NAME, CHUNKSIZE, MIN_FILE_SIZE_BYTES
from src.etl.extract import extract_by_chunks, file_size_ok
from src.etl.transform import basic_transform, count_rows
from src.etl.load import load_chunk, validate_non_empty
from src.common.logger import logger

@task(retries=2, retry_delay_seconds=120)
def wait_for_file_min_size():
    if not file_size_ok(CSV_PATH, MIN_FILE_SIZE_BYTES):
        raise ValueError("File not ready")
    return True

@task
def run_etl():
    total = 0
    first = True
    for chunk in extract_by_chunks(CSV_PATH, CHUNKSIZE):
        t = basic_transform(chunk)
        total += count_rows(t)
        load_chunk(t, SQLALCHEMY_URL, TABLE_NAME, if_exists="replace" if first else "append")
        first = False
    return total

@task
def validate_load():
    return validate_non_empty(SQLALCHEMY_URL, TABLE_NAME)

@flow(name="etl-transactions-prefect")
def etl_flow():
    wait_for_file_min_size()
    total = run_etl()
    rows = validate_load()
    logger.info(f"Prefect run done. total={total}, validated rows={rows}")

if __name__ == "__main__":
    etl_flow()
