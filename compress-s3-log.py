import os
import zipfile
import boto3
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# === AWS S3 Configuration ===
AWS_ACCESS_KEY_ID = 'minioadmin'
AWS_SECRET_ACCESS_KEY = 'minioadmin'
AWS_REGION = 'us-west-2'  # or your region
S3_ENDPOINT_URL = 'http://minio.minio.svc.cluster.local:9000'  # e.g., https://s3.wasabisys.com
BUCKET_NAME = 'test'
S3_KEY_PREFIX = 'backups/'  # Optional folder prefix in S3

# === Local log of uploaded directory identifiers ===
UPLOAD_LOG_PATH = "uploaded_paths.txt"

def load_uploaded_paths():
    """Load previously uploaded directory identifiers from local log file."""
    if not os.path.exists(UPLOAD_LOG_PATH):
        return set()
    with open(UPLOAD_LOG_PATH, 'r') as f:
        return set(line.strip() for line in f.readlines())

def save_uploaded_path(identifier):
    """Append newly uploaded directory identifier to the log file."""
    with open(UPLOAD_LOG_PATH, 'a') as f:
        f.write(identifier + "\n")

def zip_directory(source_dir, zip_file_path):
    """Recursively zip the contents of a directory."""
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for foldername, _, filenames in os.walk(source_dir):
            for filename in filenames:
                file_path = os.path.join(foldername, filename)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname)

def upload_to_s3(file_path, bucket_name, s3_key):
    """Upload a file to S3 using explicit credentials and endpoint."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        endpoint_url=S3_ENDPOINT_URL
    )
    s3_client.upload_file(file_path, bucket_name, s3_key)
    print(f"[✓] Uploaded: {file_path} → s3://{bucket_name}/{s3_key}")

def compress_and_upload(sub_dir, bucket_name, s3_key_prefix, uploaded_set):
    """Compress a subdirectory and upload it to S3, skipping if already uploaded."""
    if not os.path.isdir(sub_dir):
        return f"[!] Skipping invalid: {sub_dir}"

    parts = sub_dir.strip("/").split("/")
    name_part = "_".join(parts[-2:])  # e.g., "3_client1"

    if name_part in uploaded_set:
        return f"[SKIP] {name_part} already logged as uploaded."

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_file_name = f'{name_part}_{timestamp}.zip'
    zip_file_path = os.path.join('/tmp', zip_file_name)

    try:
        print(f"[⏳] Compressing {sub_dir} → {zip_file_path}")
        zip_directory(sub_dir, zip_file_path)
        print(f"[📁] ZIP Created: {zip_file_path}")

        s3_key = f'{s3_key_prefix}{zip_file_name}'
        upload_to_s3(zip_file_path, bucket_name, s3_key)

        os.remove(zip_file_path)
        save_uploaded_path(name_part)

        return f"[✓] Uploaded and logged: {zip_file_name}"
    except Exception as e:
        return f"[✗] Error on {sub_dir}: {e}"

def collect_subdirs(base_dirs):
    """Collect all subdirectories from a list of base paths."""
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
    subdirectories = collect_subdirs(base_dirs)

    uploaded_set = load_uploaded_paths()
    max_workers = min(8, len(subdirectories))

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(compress_and_upload, sub_dir, BUCKET_NAME, S3_KEY_PREFIX, uploaded_set)
            for sub_dir in subdirectories
        ]
        for future in as_completed(futures):
            print(future.result())

if __name__ == "__main__":
    main()
