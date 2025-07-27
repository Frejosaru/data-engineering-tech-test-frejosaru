import pandas as pd
from sqlalchemy import create_engine, text
from src.common.exceptions import LoadError, ValidationError
from src.common.logger import logger

def load_chunk(df: pd.DataFrame, conn_str: str, table: str, if_exists: str = "append"):
    try:
        engine = create_engine(conn_str)
        with engine.begin() as conn:
            df.to_sql(table, conn, if_exists=if_exists, index=False)
    except Exception as e:
        raise LoadError(f"Error loading data: {e}") from e

def validate_non_empty(conn_str: str, table: str):
    engine = create_engine(conn_str)
    with engine.begin() as conn:
        res = conn.execute(text(f"SELECT COUNT(1) FROM {table}"))
        cnt = res.scalar()
        logger.info(f"Row count in destination table '{table}': {cnt}")
        if cnt is None or cnt == 0:
            raise ValidationError(f"Destination table '{table}' is empty!")
        return cnt
