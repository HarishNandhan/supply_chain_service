# mongo_to_bigquery.py
# ------------------------------------------------------------
# Pull documents from MongoDB, flatten, and load to BigQuery.
# Reads secrets from: <repo>/supply_chain_service/configs/.env
# Env keys required:
#   MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION
#   BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE
# ------------------------------------------------------------

from pathlib import Path
import os
import re
import sys
from typing import List

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

# FastAPI app
app = FastAPI(title="MongoDB to BigQuery ETL API", version="1.0.0")


def project_root() -> Path:
    # This file lives in: <repo>/supply_chain_service/mongo_to_bigquery/
    # Project root is one level up: <repo>/supply_chain_service/
    return Path(__file__).resolve().parents[1]


def load_env() -> Path:
    dotenv_path = project_root() / "configs" / ".env"
    if not dotenv_path.exists():
        raise FileNotFoundError(f"Could not find .env at: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
    missing: List[str] = [
        k
        for k in [
            "MONGODB_URI",
            "MONGODB_DATABASE",
            "MONGODB_COLLECTION",
            "BIGQUERY_PROJECT",
            "BIGQUERY_DATASET",
            "BIGQUERY_TABLE",
        ]
        if not os.getenv(k)
    ]
    if missing:
        raise RuntimeError(
            f"Missing required env var(s): {', '.join(missing)} "
            f"(loaded from {dotenv_path})"
        )
    return dotenv_path


def sanitize_bq_column(col: str) -> str:
    """
    BigQuery column rules:
      - Only letters, numbers, and underscores
      - Must start with a letter or underscore
      - Dots from Mongo paths should become underscores
    """
    col = col.replace(".", "_")
    col = re.sub(r"[^A-Za-z0-9_]", "_", col)
    if not re.match(r"[A-Za-z_]", col):
        col = f"_{col}"
    # Avoid consecutive underscores noise
    col = re.sub(r"__+", "_", col)
    return col[:300]  # BQ allows up to 300 chars


def main() -> None:
    dotenv_path = load_env()

    MONGODB_URI = os.getenv("MONGODB_URI")
    MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
    MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")

    BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
    BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
    BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")

    print(f"[env] Loaded from: {dotenv_path}")
    print(f"[env] Mongo DB: {MONGODB_DATABASE}, Collection: {MONGODB_COLLECTION}")
    print(f"[env] BigQuery: {BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}")

    # ---- MongoDB ----
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]

    docs = list(collection.find())
    if not docs:
        print("[info] No documents found in MongoDB collection. Nothing to load.")
        return

    # Flatten nested JSON into columns
    df = pd.json_normalize(docs)

    # Ensure _id is a string
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    # Sanitize column names for BigQuery
    df.columns = [sanitize_bq_column(c) for c in df.columns]

    # Optional: convert obvious timestamps (Mongo often stores ISO strings)
    # Uncomment if you have known fields like "timestamp"
    # for col in [c for c in df.columns if "timestamp" in c.lower() or "date" in c.lower()]:
    #     with pd.option_context("mode.chained_assignment", None):
    #         df[col] = pd.to_datetime(df[col], errors="ignore")

    print(f"[dataframe] Rows: {len(df):,}  Cols: {len(df.columns):,}")

    # ---- BigQuery ----
    bq_client = bigquery.Client(project=BIGQUERY_PROJECT)

    # Ensure dataset exists
    dataset_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}"
    try:
        bq_client.get_dataset(dataset_id)
    except NotFound:
        print(f"[bq] Creating dataset: {dataset_id}")
        bq_client.create_dataset(bigquery.Dataset(dataset_id))

    table_id = f"{dataset_id}.{BIGQUERY_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # replace table each run
    )

    print(f"[bq] Loading to {table_id} ...")
    load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # wait for job to finish

    table = bq_client.get_table(table_id)
    print(f"[done] Loaded {table.num_rows:,} rows into {table_id} with {len(table.schema)} columns.")


class ETLStatus(BaseModel):
    status: str
    message: str
    rows_processed: int = 0
    columns_processed: int = 0

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "mongo-to-bigquery-etl"}

@app.post("/etl/run", response_model=ETLStatus)
async def run_etl():
    try:
        dotenv_path = load_env()

        MONGODB_URI = os.getenv("MONGODB_URI")
        MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
        MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")

        BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
        BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
        BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")

        # MongoDB
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]

        docs = list(collection.find())
        if not docs:
            return ETLStatus(
                status="success",
                message="No documents found in MongoDB collection. Nothing to load.",
                rows_processed=0,
                columns_processed=0
            )

        # Flatten nested JSON into columns
        df = pd.json_normalize(docs)

        # Ensure _id is a string
        if "_id" in df.columns:
            df["_id"] = df["_id"].astype(str)

        # Sanitize column names for BigQuery
        df.columns = [sanitize_bq_column(c) for c in df.columns]

        # BigQuery
        bq_client = bigquery.Client(project=BIGQUERY_PROJECT)

        # Ensure dataset exists
        dataset_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}"
        try:
            bq_client.get_dataset(dataset_id)
        except NotFound:
            bq_client.create_dataset(bigquery.Dataset(dataset_id))

        table_id = f"{dataset_id}.{BIGQUERY_TABLE}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # replace table each run
        )

        load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # wait for job to finish

        table = bq_client.get_table(table_id)
        
        return ETLStatus(
            status="success",
            message=f"Successfully loaded data to {table_id}",
            rows_processed=int(table.num_rows),
            columns_processed=len(table.schema)
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")

@app.get("/etl/status")
async def get_etl_status():
    try:
        dotenv_path = load_env()
        
        BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
        BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
        BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
        
        bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
        table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        
        try:
            table = bq_client.get_table(table_id)
            return {
                "table_exists": True,
                "rows": int(table.num_rows),
                "columns": len(table.schema),
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None
            }
        except NotFound:
            return {"table_exists": False, "message": "BigQuery table not found"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8004)
