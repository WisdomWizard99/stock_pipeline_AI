import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Read tickers from .env — split the comma-separated string into a list
TICKERS = os.getenv("TICKERS", "AAPL,MSFT").split(",")
PERIOD  = os.getenv("PERIOD", "1y")


def fetch_stock_data(tickers: list[str], period: str = "1y") -> pd.DataFrame:
    """
    Fetches historical OHLCV data for a list of tickers.

    Args:
        tickers: List of stock ticker symbols
        period:  How far back to pull data ("1d", "5d", "1mo", "1y", etc.)

    Returns:
        A clean DataFrame with one row per ticker per trading day
    """

    all_data = []

    for ticker in tickers:
        try:
            logger.info(f"Fetching the data for {ticker}...")
            stock = yf.Ticker(ticker)
            df = stock.history(period=period)

            if df.empty:
                logger.warning(f"No data returned for {ticker}. Skipping...")
                continue

            df = df.reset_index()
            df["ticker"] = ticker
            df["ingested_at"] = datetime.utcnow()
            df.columns = [c.lower().replace(" ", "_" ) for c in df.columns]

            all_data.append(df)
            logger.info(f"{ticker}: {len(df)} rows fetched!")

        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")

    if not all_data:
        raise ValueError("No data fetch for any ticker.")

    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total rows fetched: {len(combined)}")
    return combined    



if __name__ == "__main__":

    logger.info("=== Starting ingestion job ===")
    logger.info(f"Tickers : {TICKERS}")
    logger.info(f"Period  : {PERIOD}")

    df = fetch_stock_data(TICKERS, PERIOD)
    print(df.head())
    print(df.dtypes)

    logger.info("=== Ingestion job complete ===")