import os
import glob
import time
import shutil
import logging
import sys
import json
import requests
import pandas as pd

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ─── Setup logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ParquetIndexer")

# ─── Env config ────────────────────────────────────────────────────────────────
ES_HOST     = os.environ["ES_HOST"]
ES_INDEX    = os.environ["ES_INDEX"]
ES_URL = f"{ES_HOST}/{ES_INDEX}/_bulk" 
PARQUET_DIR = "/data/parquet"
PROCESSED   = os.path.join(PARQUET_DIR, "processed")
FAILED      = os.path.join(PARQUET_DIR, "failed")

os.makedirs(PROCESSED, exist_ok=True)
os.makedirs(FAILED,    exist_ok=True)

# ─── Spark init ───────────────────────────────────────────────────────────────
spark = (SparkSession.builder
         .appName("ParquetToES")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ─── Helper: recursively flatten StructType fields ──────────────────────────────
def flatten_spark_df(df):
    complex_fields = {
        field.name: field.dataType
        for field in df.schema.fields
        if isinstance(field.dataType, StructType)
    }

    while complex_fields:
        col_name, struct_type = complex_fields.popitem()
        expanded = [
            F.col(f"{col_name}.{nested.name}").alias(f"{col_name}_{nested.name}")
            for nested in struct_type.fields
        ]
        df = df.select(
            *[c for c in df.columns if c != col_name],
            *expanded
        )
        for nested in struct_type.fields:
            nested_col = f"{col_name}_{nested.name}"
            if isinstance(nested.dataType, StructType):
                complex_fields[nested_col] = nested.dataType
    return df

# ─── Helper: convert pandas DataFrame to NDJSON payload ────────────────────────
def df_to_ndjson(df: pd.DataFrame, index_name: str) -> str:
    lines = []
    for _, row in df.iterrows():
        meta = {"index": {"_index": index_name}}
        lines.append(json.dumps(meta))
        lines.append(row.to_json())
    return "\n".join(lines) + "\n"

# ─── Core indexing logic ───────────────────────────────────────────────────────
def index_file(path):
    fname = os.path.basename(path)

    try:
        df = spark.read.parquet(path)
        df = flatten_spark_df(df)
        pdf = df.toPandas()

        if "dropped" not in pdf.columns:
            pdf["dropped"] = False

        logger.info(f"Columns after flatten: {list(pdf.columns)}")
        logger.info(f"Sample row: {pdf.iloc[0].to_dict()}")

        # Convert to NDJSON
        ndjson_payload = df_to_ndjson(pdf, ES_INDEX)

        snippet = "\n".join(ndjson_payload.split("\n"))
        logger.info(f"Bulk payload preview:\n{snippet}")
        logger.info(f" → bulk_indexing to {ES_URL}")


        # Bulk index via HTTP
        resp = requests.post(
            ES_URL,
            headers={"Content-Type": "application/x-ndjson"},
            data=ndjson_payload.encode('utf-8')
        )

        if resp.status_code == 200 and not resp.json().get('errors'):
            logger.info(f"Indexed {len(pdf)} records from {fname}")
            shutil.move(path, os.path.join(PROCESSED, fname))
            logger.info(f"Moved {fname} → processed/")
        else:
            logger.error(f"Indexing errors for {fname}: {resp.text}")
            shutil.move(path, os.path.join(FAILED, fname))
            logger.info(f"Moved {fname} → failed/")

    except Exception as e:
        logger.error(f"ERROR indexing {fname}: {e}", exc_info=True)
        shutil.move(path, os.path.join(FAILED, fname))
        logger.info(f"Moved {fname} → failed/")

# ─── Watchdog handler ─────────────────────────────────────────────────────────
class ParquetHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".parquet"):
            index_file(event.src_path)

# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    for p in glob.glob(f"{PARQUET_DIR}/*.parquet"):
        index_file(p)

    observer = Observer()
    observer.schedule(ParquetHandler(), path=PARQUET_DIR, recursive=False)
    observer.start()
    logger.info(f"Watching {PARQUET_DIR} for new Parquet files…")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    spark.stop()
