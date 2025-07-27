import os
import pandas as pd
from src.common.logger import logger
from src.common.exceptions import ExtractError

def file_size_ok(path: str, min_size: int) -> bool:
    try:
        size = os.path.getsize(path)
        logger.info(f"File size: {size} bytes (min required: {min_size})")
        return size >= min_size
    except FileNotFoundError:
        return False

def extract_by_chunks(path: str, chunksize: int):
    """
    Generator that yields dataframes.
    Supports gzip via compression='infer'.
    """
    try:
        for chunk in pd.read_csv(path, chunksize=chunksize, compression="infer"):
            yield chunk
    except Exception as e:
        raise ExtractError(f"Error extracting data: {e}") from e
