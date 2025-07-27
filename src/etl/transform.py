import pandas as pd
from datetime import datetime
from src.common.exceptions import TransformError
from src.common.logger import logger

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def basic_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep it generic because schema is unknown. Add ingestion_ts metric column.
    """
    try:
        df = normalize_columns(df)
        df["ingestion_ts"] = datetime.utcnow()
        return df
    except Exception as e:
        raise TransformError(f"Error transforming data: {e}") from e

def count_rows(df: pd.DataFrame) -> int:
    return len(df)
