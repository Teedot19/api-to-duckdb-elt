from dagster import asset, Definitions
from ingestion_pipeline.orchestration.assets import (raw_records,validated_articles,parsed_articles_df,raw_layer_table,write_to_gsheets)
from ingestion_pipeline.resources.thenews import thenews_client
from ingestion_pipeline.resources.thenews import  duckdb
from dagster_duckdb import DuckDBResource



duckdb_resource = DuckDBResource(
    database="news_api_data.db",  
)



defs = Definitions(assets=[raw_records,
                           validated_articles,
                           parsed_articles_df,
                           raw_layer_table,
                           write_to_gsheets],
                   resources={"thenews_client": thenews_client, "duckdb": duckdb,'duckdb_connect':duckdb_resource})