import pandas as pd
import pytest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transformation.transform import (
    remove_duplicates,
    handle_nulls,
    validate_data_quality,
    reorder_columns
)


def make_sample_df():
    """Creates a minimal sample DataFrame for testing."""
    return pd.DataFrame({
        "ticker"      : ["AAPL", "AAPL", "MSFT"],
        "date"        : ["2026-01-02", "2026-01-02", "2026-01-02"],
        "open"        : [185.0, 185.0, 300.0],
        "high"        : [187.0, 187.0, 305.0],
        "low"         : [183.0, 183.0, 298.0],
        "close"       : [186.0, 186.0, 302.0],
        "volume"      : [1000000, 1000000, 500000],
        "dividends"   : [0.0, 0.0, 0.0],
        "stock_splits": [0.0, 0.0, 0.0],
        "ingested_at" : ["2026-01-02", "2026-01-02", "2026-01-02"]
    })


def test_remove_duplicates():
    """Duplicate ticker+date rows should be reduced to one."""
    df     = make_sample_df()
    result = remove_duplicates(df)
    assert len(result) == 2, "Should have 2 rows after deduplication"


def test_handle_nulls_drops_critical():
    """Rows with null close price should be dropped."""
    df           = make_sample_df()
    df.loc[0, "close"] = None
    result       = handle_nulls(df)
    assert len(result) < len(df), "Row with null close should be dropped"


def test_validate_data_quality_removes_negative_price():
    """Rows with negative prices should be removed."""
    df             = make_sample_df()
    df.loc[0, "close"] = -5.0
    result         = validate_data_quality(df)
    assert all(result["close"] > 0), "No negative prices should remain"


def test_validate_data_quality_removes_high_less_than_low():
    """Rows where high < low are physically impossible — remove them."""
    df             = make_sample_df()
    df.loc[0, "high"] = 100.0
    df.loc[0, "low"]  = 200.0
    result         = validate_data_quality(df)
    assert all(result["high"] >= result["low"]), "High must always be >= low"


def test_reorder_columns():
    """Output columns must be in the correct order."""
    df       = make_sample_df()
    result   = reorder_columns(df)
    expected = [
        "ticker", "date", "open", "high", "low",
        "close", "volume", "dividends", "stock_splits", "ingested_at"
    ]
    assert result.columns.tolist() == expected, "Column order is wrong"