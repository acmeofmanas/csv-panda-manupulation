import os
import zipfile
import boto3
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# === AWS S3 Configuration ===
AWS_ACCESS_KEY_ID = 'minioadmin'
AWS_SECRET_ACCESS_KEY = 'minioadmin'
AWS_REGION = 'us-west-2'
S3_ENDPOINT_URL = 'http://minio.minio.svc.cluster.local:9000'
BUCKET_NAME = 'test'
S3_KEY_PREFIX = 'backups/'
UPLOAD_LOG_PATH = "uploaded_paths.txt"

def load_uploaded_paths():
    if not os.path.exists(UPLOAD_LOG_PATH):
        return set()
    with open(UPLOAD_LOG_PATH, 'r') as f:
        return set(line.strip() for line in f.readlines())

def save_uploaded_paths(identifiers):
    with open(UPLOAD_LOG_PATH, 'a') as f:
        for identifier in identifiers:
            f.write(identifier + "\n")

def zip_directory(source_dir, zip_file_path):
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for foldername, _, filenames in os.walk(source_dir):
            for filename in filenames:
                file_path = os.path.join(foldername, filename)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname)

def upload_to_s3(file_path, bucket_name, s3_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        endpoint_url=S3_ENDPOINT_URL
    )
    s3_client.upload_file(file_path, bucket_name, s3_key)
    print(f"[✓] Uploaded: {file_path} → s3://{bucket_name}/{s3_key}")

def compress_and_upload(sub_dir, bucket_name, s3_key_prefix):
    if not os.path.isdir(sub_dir):
        return None, f"[!] Skipping invalid: {sub_dir}"

    parts = sub_dir.strip("/").split("/")
    name_part = "_".join(parts[-2:])  # unique identifier

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
        return name_part, f"[✓] Uploaded and logged: {zip_file_name}"
    except Exception as e:
        return None, f"[✗] Error on {sub_dir}: {e}"

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

    print(f"[🚀] Processing {len(to_process)} directories in parallel...")

    uploaded_now = []
    with ProcessPoolExecutor(max_workers=min(8, len(to_process))) as executor:
        futures = [
            executor.submit(compress_and_upload, sub_dir, BUCKET_NAME, S3_KEY_PREFIX)
            for sub_dir in to_process
        ]
        for future in as_completed(futures):
            identifier, message = future.result()
            if identifier:
                uploaded_now.append(identifier)
            print(message)

    if uploaded_now:
        save_uploaded_paths(uploaded_now)

if __name__ == "__main__":
    main()
