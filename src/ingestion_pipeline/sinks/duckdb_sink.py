from pathlib import Path
import duckdb
import pandas as pd


def write_to_duckdb(df: pd.DataFrame, db_path: Path) -> None:

    conn = duckdb.connect(str(db_path))

    # 1️⃣ Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_layer_table (
            uuid STRING,
            title STRING,
            description STRING,
            snippet STRING,
            url STRING,
            image_url STRING,
            published_at TIMESTAMP,
            source STRING,
            locale STRING,
            categories STRING,
            keywords STRING
        )
    """)

    # 2️⃣ Register incoming dataframe
    conn.register("incoming_df", df)

    # 3️⃣ Idempotent insert
    conn.execute("""
        INSERT INTO raw_layer_table
        SELECT *
        FROM incoming_df
        WHERE uuid NOT IN (
            SELECT uuid FROM raw_layer_table
        )
        AND title NOT ILIKE '%AFCON%'
        AND title NOT ILIKE '%CAN 2025%'
        AND title NOT ILIKE '%CAN-2025%'
        AND title NOT IN ('Lookman','Osimhen')
    """)

    conn.close()
