import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path
from dotenv import load_dotenv
import os

# Load .env file from project root
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def get_spark_session() -> SparkSession:
    """
    Creates or retrieves an existing SparkSession.
    getOrCreate() means safe to call multiple times —
    always returns the same session if one already exists.
    """
    spark = (
        SparkSession.builder
        .appName("StockPipelineAggregation")
        .master("local[*]")          # local mode — uses all CPU cores
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")   # suppress Spark's verbose logs
    return spark

def pandas_to_spark(pandas_df, spark: SparkSession) -> DataFrame:
    """
    Converts a pandas DataFrame to a Spark DataFrame.
    This is the bridge between my transform step (pandas)
    and my aggregation step (Spark).

    Args:
        pandas_df: clean Silver pandas DataFrame
        spark:     active SparkSession

    Returns:
        Spark DataFrame
    """
    spark_df = spark.createDataFrame(pandas_df)
    logger.info(f" Converted to Spark DataFrame — {spark_df.count()} rows")
    return spark_df

def compute_daily_return(df: DataFrame) -> DataFrame:
    """
    Computes daily percentage return per ticker using a Window function.
    Formula: (today close - yesterday close) / yesterday close * 100

    Args:
        df: Spark DataFrame sorted by ticker and date

    Returns:
        Spark DataFrame with daily_return_pct column added
    """
    logger.info("Computing daily returns...")
    
    # Window — partition by ticker, order by date
    # This ensures pct change is computed per ticker, not across tickers
    
    window_spec = ( Window.partitionBy("ticker").orderBy("date"))
    
    df = df.withColumn("prev_close", F.lag("close", 1).over(window_spec))            # previous day's close
    
    df = df.withColumn("daily_return_pct", F.round(((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100 , 4)).drop("prev_close")
    
    logger.info(" Daily returns computed")
    return df

def compute_moving_averages(df: DataFrame) -> DataFrame:
    """
    Computes 7-day and 30-day simple moving averages using Window functions.
    rowsBetween(-N, 0) means look back N rows, up to current row.

    Args:
        df: Spark DataFrame after daily return computation

    Returns:
        Spark DataFrame with ma_7 and ma_30 columns added
    """
    logger.info("Computing moving averages...")
    
    window_base = Window.partitionBy("ticker").orderBy("date")
    
    # 7-day window — look back 6 rows + current row = 7 rows total
    window_7 = window_base.rowsBetween(-6, 0)
    
    # 30-day window — look back 29 rows + current row = 30 rows total
    window_30 = window_base.rowsBetween(-29, 0)
     
    df = df.withColumn("ma_7", F.round(F.avg("close").over(window_7) , 4))
    df = df.withColumn("ma_30", F.round(F.avg("close").over(window_30) , 4))
    logger.info(" Moving averages computed")
    return df
def compute_volatility(df: DataFrame) -> DataFrame:
    """
    Computes 30-day rolling volatility — standard deviation of daily returns.
    Uses the same 30-day window as moving averages.

    Args:
        df: Spark DataFrame after moving average computation

    Returns:
        Spark DataFrame with volatility_30 column added
    """
    logger.info("Computing 30-day volatility...")

    window_30 = (
        Window
        .partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-29, 0)
    )

    df = df.withColumn(
        "volatility_30",
        F.round(F.stddev("daily_return_pct").over(window_30), 4)
    )

    logger.info(" Volatility computed")
    return df

def compute_volume_average(df: DataFrame) -> DataFrame:
    """
    Computes 30-day rolling average volume per ticker.

    Args:
        df: Spark DataFrame after volatility computation

    Returns:
        Spark DataFrame with volume_avg_30 column added
    """
    logger.info("Computing 30-day volume average...")
    
    window_30 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-29, 0)
    
    df = df.withColumn("volume_avg_30", F.round(F.avg("volume").over(window_30)  , 4))
    logger.info(" Rolling volume avg over 30 days")
    return df

def select_gold_columns(df: DataFrame) -> DataFrame:
    """
    Selects only Gold layer columns — drops raw OHLCV.

    Args:
        df: Spark DataFrame with all computed metrics

    Returns:
        Lean Gold layer Spark DataFrame
    """
    
    gold_columns = [
        "ticker",
        "date",
        "close",
        "daily_return_pct",
        "ma_7",
        "ma_30",
        "volatility_30",
        "volume_avg_30"
    ]
    
    df = df.select(gold_columns)
    logger.info("Selected Gold columns!")
    return df

def aggregate_spark(pandas_df) -> "pandas.DataFrame":
    """
    Master aggregation function using PySpark.
    Accepts a pandas DataFrame, processes in Spark,
    returns a pandas DataFrame for loading into Snowflake.

    Args:
        pandas_df: clean Silver pandas DataFrame from transform step

    Returns:
        Gold layer pandas DataFrame with computed KPIs
    """
    logger.info("=== Starting Spark aggregation ===")
    
    spark = get_spark_session()
    
    df = pandas_to_spark(pandas_df, spark)
    
    # ── Run aggregations ───────────────────────────────────────────
    df = compute_daily_return(df)
    df = compute_moving_averages(df)
    df = compute_volatility(df)
    df = compute_volume_average(df)
    df = select_gold_columns(df)
    
    # ── Convert Spark → pandas for Snowflake loading ───────────────
    gold_pandas_df = df.toPandas()

    logger.info(f"Output shape: {gold_pandas_df.shape}")
    logger.info("=== Spark aggregation complete ===")

    spark.stop()
    return gold_pandas_df

if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from transformation.extract import extract_latest_blob
    from transformation.transform import transform

    logger.info("Running Spark aggregation standalone...")
    
    raw_df = extract_latest_blob()
    clean_df = transform(raw_df)
    gold_df = aggregate_spark(clean_df)
    
    print("\n── Shape ───────────────────────────────")
    print(gold_df.shape)

    print("\n── Columns ─────────────────────────────")
    print(gold_df.columns.tolist())

    print("\n── AAPL first 5 rows ───────────────────")
    print(gold_df[gold_df["ticker"] == "AAPL"].head())

    print("\n── Summary statistics ──────────────────")
    print(gold_df.groupby("ticker")["daily_return_pct"].describe().round(3))
