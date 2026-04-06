import logging
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def cast_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fixes column data types lost during CSV round-trip.
    date and ingested_at come back as strings — convert to datetime.
    volume comes back as float — convert to int.

    Args:
        df: raw DataFrame from extract step

    Returns:
        DataFrame with correct column types
    """
    logger.info("Casting column types...")
    
    # date → proper datetime (just the date part, no time)
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.date   #convert to datetime64[us, UTC] but take only date (not time); ex.2005-01-02
    
    # ingested_at → proper datetime
    df["ingested_at"] = pd.to_datetime(df["ingested_at"])
    
    # volume → integer (it's always a whole number)
    df["volume"] = df["volume"].astype("Int64")  # capital I — handles nulls safely

    logger.info(" Column types cast successfully")
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate rows based on ticker + date combination.
    In a reliable pipeline this should never trigger — but
    defensive checks like this prevent silent data corruption.

    Args:
        df: DataFrame after type casting

    Returns:
        DataFrame with duplicates removed
    """
    before = len(df)
    df = df.drop_duplicates(subset=["ticker", "date"], keep="last")
    after = len(df)
    
    removed = after - before
    if removed > 0:
        logger.warning(f"Removed {removed} duplicate rows")
    else:
        logger.info(" No duplicates found")
        
    return df

def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles missing values across the DataFrame.
    Price and volume nulls are dropped — a row with no price is useless.
    Dividend and stock_split nulls are filled with 0 — they are
    genuinely zero on most trading days, not missing data.

    Args:
        df: DataFrame after duplicate removal

    Returns:
        DataFrame with nulls handled
    """
    logger.info("Handling nulls...")
    
    # Critical columns — drop rows where these are null
    critical_cols = ["date", "open", "high", "low", "close", "volume", "ticker"]
    before = len(df)
    df = df.dropna(subset=critical_cols)
    after  = len(df)
    
    dropped = before - after
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with nulls in critical columns")
    else:
        logger.info("✓ No critical nulls found")
        
    # Non-critical columns — fill with 0
    df["dividends"] = df["dividends"].fillna(0)
    df["stock_splits"] = df["stock_splits"].fillna(0)
    
    return df

def validate_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies business rules to catch logically invalid data.
    Prices must be positive. Volume must be non-negative.
    High must be >= Low on any given day.

    Args:
        df: DataFrame after null handling

    Returns:
        DataFrame with invalid rows removed
    """
    logger.info("Validating data quality...")

    before = len(df)
    
    # Rule 1 — prices must be positive

    df = df[
        (df["open"] > 0) &
        (df["high"] > 0) &
        (df["low"] > 0) &
        (df["close"] > 0)
    ]
    
    # Rule 2 — volume must be non-negative
    df = df[df["high"] >= df["low"]]
    
    after   = len(df)
    removed = before - after

    if removed > 0:
        logger.warning(f"Removed {removed} rows that failed data quality checks")
    else:
        logger.info("✓ All rows passed data quality checks")

    return df

def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorders columns into a clean, logical sequence.
    Identifiers first, then OHLCV, then metadata.

    Args:
        df: DataFrame after validation

    Returns:
        DataFrame with columns in clean order
    """
    
    column_order = [
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "dividends",
        "stock_splits",
        "ingested_at"
    ]
    
    df = df[column_order]
    logger.info(" Columns reordered")
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Master transformation function — runs all steps in sequence.
    This is the only function called by pipeline.py.

    Args:
        df: raw DataFrame from extract step

    Returns:
        Clean, validated Silver layer DataFrame
    """
    logger.info("=== Starting transformation ===")
    logger.info(f"Input shape: {df.shape}")

    df = cast_column_types(df)
    df = remove_duplicates(df)
    df = handle_nulls(df)
    df = validate_data_quality(df)
    df = reorder_columns(df)
    
    logger.info(f"Output shape: {df.shape}")
    logger.info("=== Transformation complete ===")
    return df


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from transformation.extract import extract_latest_blob

    logger.info("Running transform step standalone...")
    raw_df   = extract_latest_blob()
    clean_df = transform(raw_df)
    
    print("\n── Shape ───────────────────────────────")
    print(f"Before: {raw_df.shape}  →  After: {clean_df.shape}")

    print("\n── Data types after transform ──────────")
    print(clean_df.dtypes)

    print("\n── First 3 rows ────────────────────────")
    print(clean_df.head(3))

    print("\n── Null check after transform ──────────")
    print(clean_df.isnull().sum())

    print("\n── Row counts per ticker ───────────────")
    print(clean_df.groupby("ticker").size())
    