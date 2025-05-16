import os
import boto3
import shutil
import tempfile
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import zstandard as zstd
from tqdm import tqdm

# === S3 Configuration ===
AWS_ACCESS_KEY_ID = 'minioadmin'
AWS_SECRET_ACCESS_KEY = 'minioadmin'
AWS_REGION = 'us-west-2'
S3_ENDPOINT_URL = 'http://minio.minio.svc.cluster.local:9000'
BUCKET_NAME = 'test'
S3_KEY_PREFIX = 'backups/'

# === Upload tracking log ===
UPLOAD_LOG_PATH = "uploaded_paths.txt"
LOG_FILE = "backup.log"
MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100MB

# === Logging setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def log(msg):
    logging.info(msg)

def load_uploaded_paths():
    if not os.path.exists(UPLOAD_LOG_PATH):
        return set()
    with open(UPLOAD_LOG_PATH, 'r') as f:
        return set(line.strip() for line in f)

def save_uploaded_paths(identifiers):
    with open(UPLOAD_LOG_PATH, 'a') as f:
        for identifier in identifiers:
            f.write(identifier + "\n")

def compress_directory_to_zstd(source_dir, output_file):
    tar_path = f"{output_file}.tar"
    shutil.make_archive(base_name=output_file, format='tar', root_dir=source_dir)

    cctx = zstd.ZstdCompressor(level=10)
    with open(f"{output_file}.tar", 'rb') as f_in, open(output_file, 'wb') as f_out:
        cctx.copy_stream(f_in, f_out)
    os.remove(f"{output_file}.tar")

def upload_to_s3(file_path, s3_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        endpoint_url=S3_ENDPOINT_URL
    )

    config = boto3.s3.transfer.TransferConfig(multipart_threshold=MULTIPART_THRESHOLD)
    s3_client.upload_file(file_path, BUCKET_NAME, s3_key, Config=config)

def compress_and_prepare_upload(sub_dir):
    if not os.path.isdir(sub_dir):
        return None, None, None, f"[!] Invalid directory: {sub_dir}"

    parts = sub_dir.strip("/").split("/")
    name_part = "_".join(parts[-2:])
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archive_name = f"{name_part}_{timestamp}.tar.zst"
    archive_path = os.path.join(tempfile.gettempdir(), archive_name)

    try:
        compress_directory_to_zstd(sub_dir, archive_path)
        return archive_path, archive_name, name_part, f"[📦] Compressed: {archive_path}"
    except Exception as e:
        return None, None, None, f"[✗] Compression failed: {e}"

def collect_subdirs(base_dirs):
    all_subdirs = []
    for base in base_dirs:
        if os.path.isdir(base):
            for entry in os.listdir(base):
                full_path = os.path.join(base, entry)
                if os.path.isdir(full_path):
                    all_subdirs.append(full_path)
    return all_subdirs

def main():
    base_dirs = [f"/var/tmp/projects/{i}" for i in range(11)]
    all_subdirs = collect_subdirs(base_dirs)

    uploaded_set = load_uploaded_paths()
    to_process = []
    identifiers = []

    for sub_dir in all_subdirs:
        parts = sub_dir.strip("/").split("/")
        name_part = "_".join(parts[-2:])
        if name_part not in uploaded_set:
            to_process.append(sub_dir)
            identifiers.append(name_part)

    if not to_process:
        log("[ℹ️] No new directories to process. All up to date.")
        return

    log(f"[🚀] Starting compression of {len(to_process)} directories...")

    compressed_files = []
    with ProcessPoolExecutor(max_workers=min(8, len(to_process))) as executor:
        futures = {executor.submit(compress_and_prepare_upload, sub_dir): sub_dir for sub_dir in to_process}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Compressing", unit="dir"):
            archive_path, archive_name, name_part, message = future.result()
            log(message)
            if archive_path and archive_name and name_part:
                compressed_files.append((archive_path, archive_name, name_part))

    if not compressed_files:
        log("[✗] No files compressed successfully. Exiting.")
        return

    log(f"[🚀] Uploading {len(compressed_files)} files to S3...")

    uploaded_now = []

    def upload_worker(args):
        archive_path, archive_name, name_part = args
        s3_key = f"{S3_KEY_PREFIX}{archive_name}"
        try:
            upload_to_s3(archive_path, s3_key)
            log(f"[✓] Uploaded: {archive_path} → s3://{BUCKET_NAME}/{s3_key}")
            os.remove(archive_path)
            return name_part
        except Exception as e:
            log(f"[✗] Upload failed for {archive_path}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=min(8, len(compressed_files))) as upload_pool:
        futures = [upload_pool.submit(upload_worker, item) for item in compressed_files]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Uploading", unit="file"):
            uploaded = future.result()
            if uploaded:
                uploaded_now.append(uploaded)

    if uploaded_now:
        save_uploaded_paths(uploaded_now)
        log(f"[✔] Upload complete. {len(uploaded_now)} files updated in log.")
    else:
        log("[✗] No files were uploaded successfully.")

if __name__ == "__main__":
    main()
