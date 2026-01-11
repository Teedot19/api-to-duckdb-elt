from contextlib import contextmanager
from dagster_duckdb import DuckDBResource
import os





def ensure_gsheets_ready(conn, gsheet_key_path) -> None:
    """
    One-time-per-connection setup: extension + credentials secret.
    """


    if not gsheet_key_path:
        raise ValueError("GSHEET_KEY_PATH is empty. Set GOOGLE_SHEETS_KEY_FILE.")
    if not os.path.exists(gsheet_key_path):
        raise FileNotFoundError(f"Key file not found: {gsheet_key_path}")
    

    conn.execute("INSTALL gsheets FROM community;")
    conn.execute("LOAD gsheets;")


    conn.execute(f"""
            CREATE or REPLACE SECRET google_creds (
                TYPE gsheet,
                PROVIDER key_file,
                FILEPATH '{gsheet_key_path}'
            );
        """)


@contextmanager
def gsheets_connection(duckdb: DuckDBResource, gsheet_key_path):
    """
    Context manager that always returns a DuckDB connection ready for gsheets.
    """
    with duckdb.get_connection() as conn:
        ensure_gsheets_ready(conn, gsheet_key_path)
        yield conn
