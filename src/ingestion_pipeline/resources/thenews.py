# ingestion_pipeline/resources/thenews.py
from dagster import resource
from ingestion_pipeline.ingestion.api_ingestor import TheNewsApiClient
from ingestion_pipeline.config.settings import API_BASE_URL
from ingestion_pipeline.sinks.duckdb_sink import write_to_duckdb
from ingestion_pipeline.config.settings import DB_PATH

@resource
def thenews_client():
    return TheNewsApiClient(
        base_url=API_BASE_URL,
        max_retries=5,
        backoff_factor=0.5,
    )


class DuckDBWriter:
    def __init__(self, db_path):
        self.db_path = db_path

    def write(self, df):
        write_to_duckdb(df, self.db_path)

@resource
def duckdb():
    return DuckDBWriter(DB_PATH)
