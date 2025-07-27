import os

CSV_PATH = os.getenv("CSV_PATH", "/mnt/data/sample_transactions.csv")
SQLALCHEMY_URL = os.getenv("SQLALCHEMY_URL", "sqlite:////tmp/transactions.db")
TABLE_NAME = os.getenv("TABLE_NAME", "transactions")
CHUNKSIZE = int(os.getenv("CHUNKSIZE", "100000"))
MIN_FILE_SIZE_BYTES = int(os.getenv("MIN_FILE_SIZE_BYTES", "1024"))
