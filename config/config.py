import os
from dotenv import load_dotenv

load_dotenv()

# ── Azure Blob Storage ─────────────────────────────────────────────
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME    = os.getenv("AZURE_CONTAINER_NAME", "stock-data")

# ── Snowflake ──────────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "STOCK_DB")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",   "STOCK_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "STOCK_WH")

# ── Pipeline settings ──────────────────────────────────────────────
TICKERS = os.getenv("TICKERS", "AAPL,MSFT").split(",")
PERIOD  = os.getenv("PERIOD", "1y")