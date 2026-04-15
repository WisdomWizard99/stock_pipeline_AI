import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from config.config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_WAREHOUSE
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_snowflake_connection():
    """
    Creates and returns a Snowflake connection.
    Called only when needed — not at import time.

    Returns:
        Active Snowflake connection object
    """
    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        password  = SNOWFLAKE_PASSWORD,
        database  = SNOWFLAKE_DATABASE,
        schema    = SNOWFLAKE_SCHEMA,
        warehouse = SNOWFLAKE_WAREHOUSE
    )
    logger.info("Snowflake connection established")
    return conn

def prepare_dataframe_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """
    Snowflake is case-insensitive but uppercases all column names
    by default. This function uppercases column names in pandas
    to match Snowflake table definitions exactly.

    Also converts date column to string — Snowflake's write_pandas
    handles string dates more reliably than Python date objects.

    Args:
        df: DataFrame to prepare

    Returns:
        DataFrame ready for Snowflake insertion
    """
    df = df.copy()                              # never mutate the original

    # Uppercase all column names to match Snowflake convention
    df.columns = [c.upper() for c in df.columns]

    # Convert date to string if it's a date object
    if "DATE" in df.columns:
        df["DATE"] = df["DATE"].astype(str)

    return df

def truncate_table(conn, table_name: str) -> None:
    """
    Clears all rows from a table before loading fresh data.
    This implements a full refresh pattern — every pipeline run
    replaces the entire table rather than appending.

    Args:
        conn:       active Snowflake connection
        table_name: table to truncate e.g. "STOCK_PRICES_CLEAN"
    """
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        logger.info(f"Truncated table: {table_name}")
    finally:
        cursor.close()      # always close cursor even if truncate fails
        
def load_dataframe(
    df:         pd.DataFrame,
    table_name: str,
    conn
) -> None:
    """
    Loads a pandas DataFrame into a Snowflake table using write_pandas.
    write_pandas is Snowflake's official bulk load utility —
    significantly faster than row-by-row INSERT statements.

    Args:
        df:         DataFrame to load
        table_name: target Snowflake table name
        conn:       active Snowflake connection
    """
    logger.info(f"Loading {len(df)} rows into {table_name}...")

    df_prepared = prepare_dataframe_for_snowflake(df)

    success, num_chunks, num_rows, output = write_pandas(
        conn        = conn,
        df          = df_prepared,
        table_name  = table_name,
        database    = SNOWFLAKE_DATABASE,
        schema      = SNOWFLAKE_SCHEMA,
        auto_create_table = False,    # table already exists — don't recreate
        overwrite         = False     # we handle truncation ourselves
    )

    if success:
        logger.info(f"✓ Loaded {num_rows} rows into {table_name} in {num_chunks} chunk(s)")
    else:
        raise RuntimeError(f"write_pandas failed for table {table_name}. Output: {output}")


def load(silver_df: pd.DataFrame, gold_df: pd.DataFrame) -> None:
    """
    Master load function — loads both Silver and Gold DataFrames
    into Snowflake. Handles connection lifecycle and truncation.

    Args:
        silver_df: clean Silver DataFrame from transform step
        gold_df:   Gold DataFrame from aggregate step
    """
    logger.info("=== Starting load ===")

    conn = get_snowflake_connection()

    try:
        # ── Silver table ───────────────────────────────────────────
        truncate_table(conn, "STOCK_PRICES_CLEAN")
        load_dataframe(silver_df, "STOCK_PRICES_CLEAN", conn)

        # ── Gold table ─────────────────────────────────────────────
        truncate_table(conn, "STOCK_DAILY_METRICS")
        load_dataframe(gold_df, "STOCK_DAILY_METRICS", conn)

        logger.info("=== Load complete ===")

    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise       # re-raise so pipeline.py knows something went wrong

    finally:
        conn.close()
        logger.info("Snowflake connection closed")


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from transformation.extract import extract_latest_blob
    from transformation.transform import transform
    from transformation.aggregate import aggregate_spark

    logger.info("Running load step standalone...")

    raw_df    = extract_latest_blob()
    silver_df = transform(raw_df)
    gold_df   = aggregate_spark(silver_df)

    load(silver_df, gold_df)

    print("\n── Verifying rows in Snowflake ─────────")
    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM STOCK_PRICES_CLEAN")
    print(f"STOCK_PRICES_CLEAN : {cursor.fetchone()[0]} rows")

    cursor.execute("SELECT COUNT(*) FROM STOCK_DAILY_METRICS")
    print(f"STOCK_DAILY_METRICS: {cursor.fetchone()[0]} rows")

    cursor.execute("""
        SELECT ticker, COUNT(*) as rows_count
        FROM STOCK_PRICES_CLEAN
        GROUP BY ticker
        ORDER BY ticker
    """)
    print("\nRows per ticker in STOCK_PRICES_CLEAN:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} rows")

    cursor.close()
    conn.close()