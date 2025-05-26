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
ES_URL      = f"{ES_HOST}/{ES_INDEX}/_bulk"
PARQUET_DIR = "/data/parquet"
PROCESSED   = os.path.join(PARQUET_DIR, "processed")
FAILED      = os.path.join(PARQUET_DIR, "failed")

# Create control dirs
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

# ─── Utility: should we skip this path? ────────────────────────────────────────
def should_skip(path: str) -> bool:
    # skip files in processed/failed dirs
    return PROCESSED in path or FAILED in path

# ─── Core indexing logic ───────────────────────────────────────────────────────
def index_file(path):
    if should_skip(path):
        return

    fname = os.path.basename(path)
    station_id = os.path.basename(os.path.dirname(path))
    new_fname = f"{station_id}_{fname}"
    dest_processed = os.path.join(PROCESSED, new_fname)
    dest_failed    = os.path.join(FAILED,    new_fname)

    # Skip if already copied
    if os.path.exists(dest_processed) or os.path.exists(dest_failed):
        logger.info(f"Skipping {fname}: already handled as {new_fname}")
        return

    try:
        df = spark.read.parquet(path)
        df = flatten_spark_df(df)
        pdf = df.toPandas()

        if "dropped" not in pdf.columns:
            pdf["dropped"] = False

        logger.info(f"Columns after flatten: {list(pdf.columns)}")
        logger.info(f"Sample row: {pdf.iloc[0].to_dict()}")

        ndjson_payload = df_to_ndjson(pdf, ES_INDEX)
        logger.info(f" → bulk_indexing to {ES_URL}")

        resp = requests.post(
            ES_URL,
            headers={"Content-Type": "application/x-ndjson"},
            data=ndjson_payload.encode('utf-8')
        )

        if resp.status_code == 200 and not resp.json().get('errors'):
            shutil.copy2(path, dest_processed)
            logger.info(f"Indexed {len(pdf)} records from {fname}, copied → processed/ as {new_fname}")
        else:
            shutil.copy2(path, dest_failed)
            logger.error(f"Indexing errors for {fname}: {resp.text}, copied → failed/ as {new_fname}")

    except Exception as e:
        shutil.copy2(path, dest_failed)
        logger.error(f"ERROR indexing {fname}: {e}, copied → failed/ as {new_fname}", exc_info=True)

# ─── Watchdog handler ─────────────────────────────────────────────────────────
class ParquetHandler(FileSystemEventHandler):
    def __init__(self, observer):
        self.observer = observer

    def process(self, path, is_dir=False):
        if is_dir:
            # start watching any new station dir (non-recursive: files only)
            self.observer.schedule(self, path=path, recursive=False)
            logger.info(f"Now watching new directory: {path}")
        else:
            # file event
            if not should_skip(path) and path.endswith(".parquet"):
                logger.info(f"Detected file event for: {path}")
                index_file(path)

    def on_created(self, event):
        self.process(event.src_path, is_dir=event.is_directory or os.path.isdir(event.src_path))

    # def on_modified(self, event):
    #     self.process(event.src_path, is_dir=event.is_directory)

    # def on_moved(self, event):
    #     # handle both file- and dir-moves
    #     self.process(event.dest_path, is_dir=os.path.isdir(event.dest_path))


if __name__ == "__main__":
    observer = Observer()
    handler  = ParquetHandler(observer)

    # 1) initial recursive walk to handle existing files & dirs
    for root, dirs, files in os.walk(PARQUET_DIR):
        # skip control dirs
        if PROCESSED in root or FAILED in root:
            continue

        # watch any existing station dirs
        for d in dirs:
            dirpath = os.path.join(root, d)
            observer.schedule(handler, path=dirpath, recursive=False)
            logger.info(f"Watching existing directory: {dirpath}")

        # process any existing parquet files
        for f in files:
            if f.endswith(".parquet"):
                index_file(os.path.join(root, f))

    # 2) watch root for new station dirs, plus top-level file events
    observer.schedule(handler, path=PARQUET_DIR, recursive=False)
    observer.start()
    logger.info(f"Watching {PARQUET_DIR} (and new subdirectories) for Parquet files…")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    spark.stop()
