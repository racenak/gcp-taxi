import os
import urllib.request
import logging
from tqdm import tqdm
from google.cloud import storage

# Cấu hình Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Configuration ---
START_YEAR  = 2025
START_MONTH = 1
END_YEAR    = 2025
END_MONTH   = 12
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "your-bucket-name")
BASE_URL    = "https://d37ci6vzurychx.cloudfront.net"

# Khởi tạo GCS Client một lần duy nhất
storage_client = storage.Client()

def upload_to_gcs(local_path: str, bucket_name: str, blob_path: str):
    """Sử dụng client đã khởi tạo sẵn để tối ưu tốc độ."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket_name}/{blob_path}"

def download_and_upload(year: int, month: str):
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    file_url  = f"{BASE_URL}/trip-data/{file_name}"
    
    # SỬA: Bỏ dấu / ở đầu để tránh lỗi permission
    local_path = file_name 
    blob_path  = f"{year}/{file_name}"

    try:
        logger.info(f"Downloading {file_name}...")
        
        # Download với stream để tránh tốn RAM
        with urllib.request.urlopen(file_url) as response:
            file_size = int(response.info().get("Content-Length", -1))
            
            with open(local_path, "wb") as f, \
                 tqdm(desc=file_name, total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as pbar:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
                    pbar.update(len(chunk))

        # Upload
        logger.info(f"Uploading to GCS...")
        gcs_uri = upload_to_gcs(local_path, GCS_BUCKET, blob_path)
        logger.info(f"Done: {gcs_uri}")

    except Exception as e:
        logger.error(f"Error processing {file_name}: {e}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

def main():
    for year in range(START_YEAR, END_YEAR + 1):
        # Xác định tháng bắt đầu và kết thúc cho năm hiện tại
        m_start = START_MONTH if year == START_YEAR else 1
        m_end   = END_MONTH if year == END_YEAR else 12
        
        for month in range(m_start, m_end + 1):
            month_str = str(month).zfill(2)
            download_and_upload(year, month_str)

    logger.info("Process finished successfully.")

if __name__ == "__main__":
    main()