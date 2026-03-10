import os
import requests
import logging
from google.cloud import storage

# Cấu hình Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Configuration ---
START_YEAR  = 2025
START_MONTH = 1
END_YEAR    = 2025
END_MONTH   = 12
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "nyc-taxi-raw-dataset")
BASE_URL    = "https://d37ci6vzurychx.cloudfront.net"

# Khởi tạo GCS Client
storage_client = storage.Client()

def stream_download_and_upload(year: int, month: str):
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    file_url  = f"{BASE_URL}/trip-data/{file_name}"
    blob_path = f"{year}/{file_name}"

    try:
        logger.info(f"Starting stream download & upload for {file_name}...")

        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(blob_path)

        # Download with streaming
        with requests.get(file_url, stream=True, timeout=300) as response:
            response.raise_for_status()  # Raise exception for bad status codes (4xx/5xx)

            # Stream directly to GCS — .raw is the file-like object
            blob.upload_from_file(
                response.raw,
                content_type="application/octet-stream",
                rewind=False  # Important: don't seek, since we're streaming
            )

        logger.info(f"Successfully uploaded to gs://{GCS_BUCKET}/{blob_path}")

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error for {file_name}: {http_err.response.status_code} - {http_err.response.reason}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request failed for {file_name}: {req_err}")
    except Exception as e:
        logger.error(f"Error processing {file_name}: {str(e)}", exc_info=True)

def main():
    for year in range(START_YEAR, END_YEAR + 1):
        m_start = START_MONTH if year == START_YEAR else 1
        m_end   = END_MONTH if year == END_YEAR else 12

        for month in range(m_start, m_end + 1):
            month_str = f"{month:02d}"
            stream_download_and_upload(year, month_str)

    logger.info("All downloads and uploads finished.")

if __name__ == "__main__":
    main()