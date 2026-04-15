import logging
import pandas as pd
import snowflake.connector
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

    Returns:
        Active Snowflake connection object
    """
    conn = snowflake.connector.connect(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        password  = SNOWFLAKE_PASSWORD,
        database  = SNOWFLAKE_DATABASE,
        schema    = SNOWFLAKE_SCHEMA,
        warehouse = SNOWFLAKE_WAREHOUSE
    )
    logger.info(" Snowflake connection established")
    return conn

def fetch_latest_metrics() -> pd.DataFrame:
    """
    Fetches the most recent metrics for each ticker
    from the Gold table — one row per ticker showing
    their latest KPIs.

    Returns:
        DataFrame with latest metrics per ticker
    """
    query = """
        SELECT
            ticker,
            MAX(date)                AS latest_date,
            MAX(close)               AS latest_close,
            AVG(daily_return_pct)    AS avg_daily_return,
            MAX(ma_7)                AS latest_ma7,
            MAX(ma_30)               AS latest_ma30,
            MAX(volatility_30)       AS latest_volatility,
            MAX(volume_avg_30)       AS latest_volume_avg
        FROM STOCK_DAILY_METRICS
        GROUP BY ticker
        ORDER BY avg_daily_return DESC
    """
    logger.info("Fetching latest metrics from Snowflake...")

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows    = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df      = pd.DataFrame(rows, columns=columns)
        logger.info(f"✓ Fetched metrics for {len(df)} tickers")
        return df

    finally:
        cursor.close()
        conn.close()
        
def fetch_recent_performance(days: int = 30) -> pd.DataFrame:
    """
    Fetches daily metrics for the last N days
    across all tickers — used for deep dive analysis.

    Args:
        days: how many recent trading days to fetch

    Returns:
        DataFrame with recent daily metrics per ticker
    """
    query = f"""
        SELECT
            ticker,
            date,
            close,
            daily_return_pct,
            ma_7,
            ma_30,
            volatility_30
        FROM STOCK_DAILY_METRICS
        WHERE date >= DATEADD(day, -{days}, CURRENT_DATE())
        ORDER BY ticker, date
    """
    logger.info(f"Fetching last {days} days of performance...")

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows    = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df      = pd.DataFrame(rows, columns=columns)
        logger.info(f"✓ Fetched {len(df)} rows of recent performance")
        return df

    finally:
        cursor.close()
        conn.close()
        
def fetch_top_movers() -> pd.DataFrame:
    """
    Fetches the top 3 best and worst performing tickers
    by average daily return — used for dashboard summary cards.

    Returns:
        DataFrame with top and bottom performers
    """
    query = """
        SELECT
            ticker,
            ROUND(AVG(daily_return_pct), 3)   AS avg_return,
            ROUND(MAX(volatility_30), 3)       AS volatility,
            MAX(close)                         AS latest_close
        FROM STOCK_DAILY_METRICS
        GROUP BY ticker
        ORDER BY avg_return DESC
    """
    logger.info("Fetching top movers...")

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows    = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df      = pd.DataFrame(rows, columns=columns)
        logger.info(f"✓ Fetched top movers for {len(df)} tickers")
        return df

    finally:
        cursor.close()
        conn.close()


def format_metrics_for_llm(metrics_df: pd.DataFrame) -> str:
    """
    Converts the metrics DataFrame into a clean text summary
    that an LLM can understand and reason about.

    This is called prompt context — structured data formatted
    as readable text before being sent to the LLM. The quality
    of this formatting directly affects the quality of AI responses.

    Args:
        metrics_df: DataFrame from fetch_latest_metrics()

    Returns:
        Formatted string ready to inject into LLM prompt
    """
    lines = [
        "LATEST STOCK METRICS SUMMARY",
        "=" * 40
    ]

    for _, row in metrics_df.iterrows():
        trend = "Bullish" if row["LATEST_MA7"] > row["LATEST_MA30"] else "Bearish"

        lines.append(f"""
Ticker          : {row['TICKER']}
Latest Close    : ${row['LATEST_CLOSE']:.2f}
Avg Daily Return: {row['AVG_DAILY_RETURN']:.3f}%
MA7  (short)    : ${row['LATEST_MA7']:.2f}
MA30 (long)     : ${row['LATEST_MA30']:.2f}
Volatility (30d): {row['LATEST_VOLATILITY']:.3f}%
Avg Volume (30d): {int(row['LATEST_VOLUME_AVG']):,}
Trend Signal    : {trend}
{'-' * 40}""")

    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Test all three queries
    logger.info("=== Testing query_snowflake.py ===")

    print("\n── Latest Metrics ──────────────────────")
    metrics_df = fetch_latest_metrics()
    print(metrics_df)

    print("\n── Recent Performance (last 30 days) ───")
    recent_df = fetch_recent_performance(days=30)
    print(f"Shape: {recent_df.shape}")
    print(recent_df.head(3))

    print("\n── Top Movers ──────────────────────────")
    movers_df = fetch_top_movers()
    print(movers_df)

    print("\n── Formatted for LLM ───────────────────")
    print(format_metrics_for_llm(metrics_df))