from dagster import asset,AssetMaterialization
from ingestion_pipeline.ingestion.api_ingestor import TheNewsApiClient
from ingestion_pipeline.config.settings import API_BASE_URL, TOP_NEWS_ENDPOINT, DEFAULT_PARAMS, GSHEET_KEY,GSHEET_FILE
from ingestion_pipeline.models.news_article import NewsArticle
from ingestion_pipeline.transforms.pandas_sync import articles_to_dataframe
from ingestion_pipeline.sinks.duckdb_sink import write_to_duckdb
from ingestion_pipeline.config.settings import DB_PATH
from dagster_duckdb import DuckDBResource
from ingestion_pipeline.config.duckdb_gsheets import gsheets_connection

GSHEET_KEY_PATH = GSHEET_KEY
GSHEET_FILE_PATH = GSHEET_FILE



@asset(required_resource_keys={"thenews_client"})
def raw_records(context):
    client = context.resources.thenews_client

    return list(
        client.fetch_paginated(
            endpoint=TOP_NEWS_ENDPOINT,
            params=DEFAULT_PARAMS,
            max_pages=60,
        )
    )


@asset
def validated_articles(raw_records):
    return NewsArticle.from_list(raw_records)

@asset
def parsed_articles_df(validated_articles):
    return articles_to_dataframe(validated_articles)



@asset(required_resource_keys={"duckdb"})
def raw_layer_table(context, parsed_articles_df):
    context.resources.duckdb.write(parsed_articles_df)

    context.log_event(
        AssetMaterialization(
            asset_key="raw_layer_table",
            metadata={"rows": len(parsed_articles_df)},
        )
    )




@asset(deps=[raw_layer_table])
def write_to_gsheets(duckdb_connect: DuckDBResource):

    with gsheets_connection(duckdb_connect, GSHEET_KEY_PATH) as conn:
        
        conn.execute("CREATE OR REPLACE TABLE looker_data AS SELECT * FROM curated_layer_view;")

        conn.execute(f"""
            COPY looker_data
            TO  '{GSHEET_FILE_PATH}' 
            (FORMAT gsheet, SECRET 'google_creds', SHEET 'Sheet1',OVERWRITE_SHEET true)
        """)
