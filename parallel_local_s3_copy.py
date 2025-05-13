import os
import zipfile
import boto3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def zip_directory(source_dir, zip_file_path):
    """Recursively zip the contents of a directory."""
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for foldername, subfolders, filenames in os.walk(source_dir):
            for filename in filenames:
                file_path = os.path.join(foldername, filename)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname)

def upload_to_s3(file_path, bucket_name, s3_key):
    """Upload a file to an S3 bucket."""
    s3_client = boto3.client('s3')
    s3_client.upload_file(file_path, bucket_name, s3_key)
    print(f"[✓] Uploaded {file_path} → s3://{bucket_name}/{s3_key}")

def process_directory(source_dir, bucket_name, s3_key_prefix):
    """Compress and upload a single directory."""
    if not os.path.isdir(source_dir):
        print(f"[!] Skipping: {source_dir} is not a valid directory.")
        return

    base_name = os.path.basename(os.path.normpath(source_dir))
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_file_name = f'{base_name}_{timestamp}.zip'
    zip_file_path = os.path.join('/tmp', zip_file_name)

    print(f"[⏳] Zipping {source_dir} → {zip_file_path}")
    zip_directory(source_dir, zip_file_path)

    s3_key = f'{s3_key_prefix}{zip_file_name}'
    upload_to_s3(zip_file_path, bucket_name, s3_key)

    # Optional cleanup
    os.remove(zip_file_path)

def main():
    # ✅ List of directories to backup
    source_directories = [
        '/path/to/source1',
        '/path/to/source2/deep',
        '/path/source3'
    ]

    # ✅ Common S3 bucket and prefix
    bucket_name = 'your-s3-bucket-name'
    s3_key_prefix = 'backups/'

    # ✅ Set number of threads based on your system/network capacity
    max_workers = min(8, len(source_directories))  # Limit to 8 threads or less

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_directory, source, bucket_name, s3_key_prefix)
            for source in source_directories
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[✗] Error: {e}")

if __name__ == "__main__":
    main()
