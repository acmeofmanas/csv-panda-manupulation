import os
import zipfile
import boto3
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

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

def compress_and_upload(source_dir, bucket_name, s3_key_prefix):
    """Compress and upload a single directory. To be run in a separate process."""
    if not os.path.isdir(source_dir):
        return f"[!] Skipping: {source_dir} is not a valid directory."

    base_name = os.path.basename(os.path.normpath(source_dir))
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_file_name = f'{base_name}_{timestamp}.zip'
    zip_file_path = os.path.join('/tmp', zip_file_name)

    try:
        print(f"[⏳] Zipping {source_dir} → {zip_file_path}")
        zip_directory(source_dir, zip_file_path)

        s3_key = f'{s3_key_prefix}{zip_file_name}'
        upload_to_s3(zip_file_path, bucket_name, s3_key)

        os.remove(zip_file_path)
        return f"[✓] Done: {source_dir} → {s3_key}"
    except Exception as e:
        return f"[✗] Failed for {source_dir}: {e}"

def main():
    # ✅ List of directories to back up
    source_directories = [
        '/path/to/source1',
        '/path/to/source2',
        '/path/to/source3'
    ]

    # ✅ Common S3 configuration
    bucket_name = 'your-s3-bucket-name'
    s3_key_prefix = 'backups/'

    max_workers = min(4, len(source_directories))  # Adjust based on CPU cores

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(compress_and_upload, source, bucket_name, s3_key_prefix)
            for source in source_directories
        ]
        for future in as_completed(futures):
            print(future.result())

if __name__ == "__main__":
    main()
