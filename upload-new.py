import os
import sys
import time
import logging
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# === Configuration ===
WATCH_DIRS = ["./output1", "./output2","./output3","./output4"]
BUCKET_NAME = "test"
S3_PREFIX = "uploads"
S3_ENDPOINT = "http://minio.minio.svc.cluster.local:9000"
AWS_ACCESS_KEY = "minioadmin"
AWS_SECRET_KEY = "minioadmin"
REGION_NAME = "us-east-1"
SCAN_INTERVAL = 60  # seconds
FILE_AGE_HOURS = 1  # Only upload files older than this
MAX_RETRIES = 3     # Upload retry attempts

# === Logging Setup ===
LOG_FILE = "s3_upload.log"
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

# === Redirect stdout/stderr to logger ===
class StreamToLogger:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass

sys.stdout = StreamToLogger(logger, logging.INFO)
sys.stderr = StreamToLogger(logger, logging.ERROR)

# === Upload Progress Logger ===
class UploadProgressLogger:
    def __init__(self, filename, filesize):
        self.filename = filename
        self._filesize = filesize
        self._seen_so_far = 0
        self._last_logged_percent = -1

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        percent = int((self._seen_so_far / self._filesize) * 100)
        if percent != self._last_logged_percent and percent % 10 == 0:
            logger.info(f"⬆️  Uploading {self.filename}: {percent}%")
            self._last_logged_percent = percent

# === Initialize S3 client ===
try:
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION_NAME
    )
    logger.info("✅ S3 client initialized.")
except Exception as e:
    logger.error(f"❌ Failed to initialize S3 client: {e}")
    raise SystemExit(1)

# === Utility Function to Check File Age ===
def is_older_than(filepath, hours):
    try:
        file_mtime = os.path.getmtime(filepath)
        file_datetime = datetime.fromtimestamp(file_mtime)
        return file_datetime < (datetime.now() - timedelta(hours=hours))
    except Exception as e:
        logger.warning(f"⚠️ Could not get file age for {filepath}: {e}")
        return False

# === Upload with Retry ===
def upload_file(filepath, source_dir):
    filename = os.path.basename(filepath)
    rel_dir = os.path.basename(os.path.normpath(source_dir))
    key = f"{S3_PREFIX}/{rel_dir}/{filename}"

    filesize = os.path.getsize(filepath)
    progress = UploadProgressLogger(filename, filesize)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            s3.upload_file(
                Filename=filepath,
                Bucket=BUCKET_NAME,
                Key=key,
                Callback=progress
            )
            logger.info(f"✅ Uploaded: {filepath} to s3://{BUCKET_NAME}/{key}")
            try:
                os.remove(filepath)
                logger.info(f"🗑️  Deleted local file: {filepath}")
            except Exception as delete_error:
                logger.warning(f"⚠️ Could not delete {filepath}: {delete_error}")
            break  # Exit retry loop after success

        except ClientError as e:
            logger.error(f"❌ Attempt {attempt} failed for {filepath}: {e}")
            if attempt == MAX_RETRIES:
                logger.error(f"❌ Giving up on {filepath} after {MAX_RETRIES} attempts.")

# === Main Loop ===
def main():
    logger.info("🚀 Watching directories for .gz files older than 5 hours...")
    while True:
        for watch_dir in WATCH_DIRS:
            if not os.path.isdir(watch_dir):
                logger.warning(f"⚠️ Skipping invalid directory: {watch_dir}")
                continue

            try:
                for fname in os.listdir(watch_dir):
                    if not fname.endswith(".gz"):
                        continue

                    full_path = os.path.join(watch_dir, fname)

                    if os.path.isfile(full_path) and is_older_than(full_path, FILE_AGE_HOURS):
                        upload_file(full_path, watch_dir)

            except Exception as e:
                logger.error(f"🚨 Error scanning {watch_dir}: {e}")

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    main()
