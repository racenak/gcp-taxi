import os
import urllib.request
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
    blob_path  = f"{year}/{file_name}"

    try:
        logger.info(f"Starting stream for {file_name}...")
        
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(blob_path)

        # Mở kết nối đến URL
        with urllib.request.urlopen(file_url) as response:
            # Dùng upload_from_file để truyền stream trực tiếp
            # Google Cloud Client sẽ tự xử lý việc chia nhỏ (chunking)
            blob.upload_from_file(response, content_type='application/octet-stream')
            
        logger.info(f"Successfully streamed to gs://{GCS_BUCKET}/{blob_path}")

    except Exception as e:
        logger.error(f"Error processing {file_name}: {e}")

def main():
    for year in range(START_YEAR, END_YEAR + 1):
        m_start = START_MONTH if year == START_YEAR else 1
        m_end   = END_MONTH if year == END_YEAR else 12
        
        for month in range(m_start, m_end + 1):
            month_str = str(month).zfill(2)
            stream_download_and_upload(year, month_str)

    logger.info("All processes finished.")

if __name__ == "__main__":
    main()