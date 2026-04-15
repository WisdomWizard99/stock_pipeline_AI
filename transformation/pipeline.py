import logging
import sys
from datetime import datetime
import os
# Add project root to path so all modules are findable
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def run_pipeline():
    """
    Master orchestrator — runs the full pipeline end to end:
    Ingest → Extract → Transform → Aggregate → Load

    Each step is isolated in a try/except so failures are
    clearly identified and the pipeline fails loudly with
    a descriptive message rather than a cryptic traceback.
    """
    pipeline_start = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("STOCK PIPELINE — STARTING")
    logger.info(f"Run started at: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info("=" * 60)
    
    # ── Step 1 — Ingest: yfinance → Azure Blob ────────────────────
    logger.info("\n[STEP 1/4] Ingestion — yfinance → Azure Blob Storage")
    try:
        from ingestion.fetch_stocks import fetch_stock_data
        from ingestion.upload_to_blob import upload_dataframe_to_blob, build_blob_name
        from config.config import TICKERS, PERIOD

        raw_df    = fetch_stock_data(TICKERS, PERIOD)
        blob_name = build_blob_name(prefix="raw")
        url       = upload_dataframe_to_blob(raw_df, blob_name)

        logger.info(f" Step 1 complete — {len(raw_df)} rows landed at {blob_name}")
    except Exception as e:
        logger.error(f" Step 1 FAILED — Ingestion: {e}")
        sys.exit(1)     # stop pipeline — no point transforming if ingestion failed

    # ── Step 2 — Extract: Azure Blob → pandas DataFrame ───────────
    logger.info("\n[STEP 2/4] Extraction — Azure Blob → pandas DataFrame")
    try:
        from transformation.extract import extract_latest_blob

        extracted_df = extract_latest_blob()
        logger.info(f" Step 2 complete — {len(extracted_df)} rows extracted")

    except Exception as e:
        logger.error(f" Step 2 FAILED — Extraction: {e}")
        sys.exit(1)

    # ── Step 3 — Transform: clean → Silver DataFrame ──────────────
    logger.info("\n[STEP 3/4] Transformation — clean + aggregate")
    try:
        from transformation.transform import transform
        from transformation.aggregate_spark import aggregate_spark

        silver_df = transform(extracted_df)
        gold_df   = aggregate_spark(silver_df)

        logger.info(f" Step 3 complete — Silver: {silver_df.shape}, Gold: {gold_df.shape}")

    except Exception as e:
        logger.error(f" Step 3 FAILED — Transformation: {e}")
        sys.exit(1)

    # ── Step 4 — Load: Silver + Gold → Snowflake ──────────────────
    logger.info("\n[STEP 4/4] Load — Snowflake Silver + Gold tables")
    try:
        from transformation.load import load

        load(silver_df, gold_df)
        logger.info(f" Step 4 complete — data loaded into Snowflake")

    except Exception as e:
        logger.error(f" Step 4 FAILED — Load: {e}")
        sys.exit(1)

    # ── Pipeline summary ───────────────────────────────────────────
    pipeline_end      = datetime.utcnow()
    duration_seconds  = (pipeline_end - pipeline_start).seconds

    logger.info("\n" + "=" * 60)
    logger.info("STOCK PIPELINE — COMPLETE")
    logger.info(f"Duration        : {duration_seconds} seconds")
    logger.info(f"Rows ingested   : {len(raw_df)}")
    logger.info(f"Rows in Silver  : {len(silver_df)}")
    logger.info(f"Rows in Gold    : {len(gold_df)}")
    logger.info(f"Blob path       : {blob_name}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()