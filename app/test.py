import os
import requests
from google.cloud import storage

# ── CONFIG ────────────────────────────────────────────────────────────────────
FILE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
LOCAL_FILENAME = "yellow_tripdata_2025-01.parquet"

GCS_BUCKET = os.getenv("GCS_BUCKET", "your-bucket-name")          # ← change me
GCS_DEST_PATH = os.getenv("GCS_DEST_PATH", f"trip-data/{LOCAL_FILENAME}")
# ─────────────────────────────────────────────────────────────────────────────


def download_file(url: str, local_path: str) -> None:
    """Stream-download a file from *url* to *local_path*."""
    print(f"Downloading {url} …")
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        downloaded = 0
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):  # 8 MB
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"  {downloaded / 1e6:.1f} MB / {total / 1e6:.1f} MB  ({pct:.1f}%)", end="\r")
    print(f"\nSaved to {local_path}  ({os.path.getsize(local_path) / 1e6:.1f} MB)")


def upload_to_gcs(local_path: str, bucket_name: str, destination_blob: str) -> None:
    """Upload *local_path* to gs://<bucket_name>/<destination_blob>."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    print(f"Uploading to gs://{bucket_name}/{destination_blob} …")
    blob.upload_from_filename(local_path)
    print("Upload complete.")


def main() -> None:
    # 1. Download
    download_file(FILE_URL, LOCAL_FILENAME)

    # 2. Upload to GCS
    upload_to_gcs(LOCAL_FILENAME, GCS_BUCKET, GCS_DEST_PATH)

    # 3. (Optional) remove local file after upload
    os.remove(LOCAL_FILENAME)
    print(f"Local file '{LOCAL_FILENAME}' removed.")


if __name__ == "__main__":
    main()
