import os
import logging
import pandas as pd
from io import StringIO
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME    = os.getenv("AZURE_CONTAINER_NAME", "stock-data")


def get_container_client():
    """
    Creates and returns a fresh container client.
    Called only when needed — not at import time.
    """
    if CONNECTION_STRING is None:
        raise EnvironmentError(
            "AZURE_STORAGE_CONNECTION_STRING not found. Check your .env file."
        )
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    return  blob_service_client.get_container_client(CONTAINER_NAME)


def list_blobs(name_starts_with: str = "raw/"):
    """
    Lists all blob files inside a given prefix/folder in the container.

    Args:
        prefix: folder path inside container e.g. "raw/"

    Returns:
        List of blob names sorted chronologically
    """
    
    container_client = get_container_client()
    blobs = [
        blob.name for blob in container_client.list_blobs(name_starts_with=name_starts_with)
    ]

    blobs.sort()        # chronological order — timestamps in names sort naturally
    logger.info(f"Found {len(blobs)} blobs under prefix '{name_starts_with}'")
    return blobs

def read_blob_to_dataframe(blob_name: str) -> pd.DataFrame:
    """
    Reads a single CSV blob from Azure Blob Storage into a pandas DataFrame.

    Args:
        blob_name: full blob path e.g. "raw/2026-04-02_05-19-42.csv"

    Returns:
        pandas DataFrame with raw stock data
    """
    if CONNECTION_STRING is None:
        raise EnvironmentError(
            "AZURE_STORAGE_CONNECTION_STRING not found. Check your .env file."
        )

    logger.info(f"Reading blob: {blob_name}")

    #Connect and get blob client
    container_client = get_container_client()
    blob_client= container_client.get_blob_client(blob_name)
    
    #Download blob content as bytes
    blob_data = blob_client.download_blob()
    content = blob_data.readall()
    
    #Decode bytes → string → DataFrame
    content_str = content.decode("utf-8")
    df = pd.read_csv(StringIO(content_str))
    
    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns from {blob_name}")
    return df

def extract_latest_blob() -> pd.DataFrame:
    """
    Convenience function — always extracts the most recent raw blob.

    Returns:
        DataFrame from the most recently ingested blob
    """
    blobs = list_blobs(name_starts_with="raw/")
    
    if not blobs:
        raise FileNotFoundError("No blobs found under 'raw/' prefix. ")
    
    latest_blob = blobs[-1]  # last item = most recent timestamp
    logger.info(f"Latest blob identified: {latest_blob}")
    
    return read_blob_to_dataframe(latest_blob)




if __name__ == "__main__":
    logger.info("Starting Extraction ... ")
    df = extract_latest_blob()