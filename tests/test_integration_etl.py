import pandas as pd
from src.etl.extract import extract_by_chunks
from src.etl.transform import basic_transform
from src.etl.load import load_chunk, validate_non_empty

def test_full_etl_integration(tmp_path):
    # Crear CSV temporal
    csv_temp = tmp_path / "s.csv"
    pd.DataFrame({"id":[1,2,3], "val":[10,20,30]}).to_csv(csv_temp, index=False)

    # Param
    chunksize = 2
    conn = f"sqlite:///{tmp_path}/db.db"

    first = True
    total = 0
    for chunk in extract_by_chunks(str(csv_temp), chunksize):
        t = basic_transform(chunk)
        total += len(t)
        load_chunk(t, conn, "t", if_exists="replace" if first else "append")
        first = False

    assert validate_non_empty(conn, "t") == total
