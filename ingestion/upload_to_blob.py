import os
import logging
from io import StringIO
from datetime import datetime

import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# Read Azure credentials from .env
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME",  "stock-data")

def upload_dataframe_to_blob(df : pd.DataFrame, blob_name: str) -> str:
    """
    Uploads a pandas DataFrame as a CSV file to Azure Blob Storage.

    Args:
        df:        The DataFrame to upload
        blob_name: Path/filename inside the container e.g. "raw/2024-03-15.csv"

    Returns:
        The full blob URL of the uploaded file
    """

    if CONNECTION_STRING is None:
        raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING not found in environment. Check your .env file.")
    
    #Step A: Convert DataFrame → CSV string in memory

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()                                # to dodge the cursor problem

    logger.info(f"DataFrame converted to CSV — {len(csv_content)} bytes")

    #Step B: Connect to Azure
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blob_client = container_client.get_blob_client(blob_name)

    #Step C: Upload
    blob_client.upload_blob(csv_content, overwrite=True, content_type="text/csv")

    blob_url = blob_client.url
    logger.info(f"Uploaded to Azure Blob: {blob_url}")
    return blob_url

def build_blob_name(prefix: str = "raw") -> str:
    """
    Builds a timestamped file path for the blob.
    e.g. "raw/2024-03-15_09-32-11.csv"

    Args:
        prefix: folder name inside container — "raw" for Bronze layer

    Returns:
        A string blob path
    """

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{prefix}/{timestamp}.csv"


if __name__ == "__main__":
    # Import fetch function from sibling module
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   #project root  
    from ingestion.fetch_stocks import fetch_stock_data

    TICKERS = os.getenv("TICKERS", "AAPL,MSFT").split(",")
    PERIOD  = os.getenv("PERIOD", "1y")

    # Fetch from Yahoo Finance → RAM
    logger.info(" Starting ingestion + upload job ...")
    df = fetch_stock_data(TICKERS, PERIOD)
    
    # Build a timestamped blob name
    blob_name = build_blob_name(prefix="raw")
    logger.info(f"Target blob path: {blob_name}")

    # Upload RAM → Azure Blob Storage
    url = upload_dataframe_to_blob(df, blob_name)

    print(f"\nData successfully landed in Bronze layer")
    print(f"  Rows     : {len(df)}")
    print(f"  Blob path: {blob_name}")
    print(f"  URL      : {url}")