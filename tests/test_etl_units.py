import pandas as pd
from src.etl.transform import basic_transform
from src.etl.load import validate_non_empty, load_chunk
from sqlalchemy import create_engine, text

def test_basic_transform_adds_ingestion_ts():
    df = pd.DataFrame({"A": [1, 2]})
    out = basic_transform(df)
    assert "ingestion_ts" in out.columns

def test_load_and_validate_sqlite(tmp_path):
    db_path = tmp_path / "t.db"
    conn = f"sqlite:///{db_path}"
    df = pd.DataFrame({"a": [1, 2]})
    load_chunk(df, conn, "t", if_exists="replace")
    count = validate_non_empty(conn, "t")
    assert count == 2
