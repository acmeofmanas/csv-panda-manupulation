import os
import sys
import time
import logging
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# === Configuration ===
WATCH_DIRS = ["./output11", "./output22","./output33"]
BUCKET_NAME = "upload-files" # <--- IMPORTANT: Change this
S3_PREFIX = "uploads"
S3_ENDPOINT = "http://minio.minio.svc.cluster.local:9000" # <--- IMPORTANT: Change this if not using local MinIO
AWS_ACCESS_KEY = "minioadmin"       # <--- IMPORTANT: Change this
AWS_SECRET_KEY = "minioadmin"       # <--- IMPORTANT: Change this
REGION_NAME = "us-east-1"
SCAN_INTERVAL = 60 # seconds
FILE_AGE_HOURS = 5 # Only upload files older than this
MAX_RETRIES = 3    # Upload retry attempts

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
        # Process buffer line by line to ensure complete lines are logged
        self.linebuf += buf
        while '\n' in self.linebuf:
            line, self.linebuf = self.linebuf.split('\n', 1)
            self.logger.log(self.level, line.rstrip())

    def flush(self):
        # Log any remaining content in the buffer
        if self.linebuf:
            self.logger.log(self.level, self.linebuf.rstrip())
            self.linebuf = ''


# Temporarily store original streams
original_stdout = sys.stdout
original_stderr = sys.stderr

try:
    sys.stdout = StreamToLogger(logger, logging.INFO)
    sys.stderr = StreamToLogger(logger, logging.ERROR)
    logger.info("✅ Stdout and Stderr redirected to logger.")
except Exception as e:
     # Fallback print in case logger setup fails
     print(f"❌ Failed to redirect stdout/stderr: {e}", file=original_stderr)


# === Upload Progress Logger ===
class UploadProgressLogger:
    def __init__(self, filename, filesize):
        self.filename = filename
        self._filesize = filesize
        self._seen_so_far = 0
        self._last_logged_percent = -1

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        # Calculate percentage, handle division by zero for empty files
        percent = int((self._seen_so_far / self._filesize) * 100) if self._filesize > 0 else 100

        # Log progress every 10% or when 100% is reached
        if percent != self._last_logged_percent and (percent % 10 == 0 or percent == 100):
            logger.info(f"⬆️  Uploading {self.filename}: {percent}%")
            self._last_logged_percent = percent

# === Initialize S3 client ===
s3 = None # Initialize to None
try:
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION_NAME
    )
    # Attempt a simple operation to check connection (optional but good practice)
    # s3.list_buckets()
    logger.info(f"✅ S3 client initialized and connected to {S3_ENDPOINT}.")
except Exception as e:
    logger.error(f"❌ Failed to initialize or connect S3 client at {S3_ENDPOINT}: {e}")
    # Allow the script to start but connection errors will occur later during uploads
    # raise SystemExit(1) # Could exit here if connection is critical upfront

# === Utility Function to Check File Age ===
def is_older_than(filepath, hours):
    """Checks if a file's last modification time is older than N hours."""
    try:
        # Check if the file exists before getting mtime
        if not os.path.exists(filepath):
             logger.warning(f"⚠️ File disappeared before age check: {filepath}")
             return False # Or True, depending on desired behavior; False is safer.

        file_mtime = os.path.getmtime(filepath)
        file_datetime = datetime.fromtimestamp(file_mtime)
        age_threshold = datetime.now() - timedelta(hours=hours)
        return file_datetime < age_threshold
    except Exception as e:
        # Log potential issues like permissions errors
        logger.warning(f"⚠️ Could not get file age for {filepath}: {e}")
        return False # Treat error as not older

# === Upload with Retry ===
def upload_file(filepath, watch_dir):
    """Uploads a file to S3 with retries and preserves subdirectory structure."""
    if s3 is None:
        logger.error(f"❌ S3 client not initialized. Skipping upload for {filepath}.")
        return

    # Calculate the relative path from the original watch_dir
    # This preserves the subdirectory structure in the S3 key
    try:
        relative_path = os.path.relpath(filepath, watch_dir)
    except ValueError as e:
        logger.error(f"❌ Could not calculate relative path for {filepath} from {watch_dir}: {e}. Skipping.")
        return

    # Construct the S3 key using the prefix and relative path
    # Replace Windows backslashes with forward slashes for S3 keys
    key = os.path.join(S3_PREFIX, relative_path).replace("\\", "/")

    filename = os.path.basename(filepath) # Use basename for logging clarity

    try:
        filesize = os.path.getsize(filepath)
        if filesize == 0:
             logger.info(f" Skipping empty file: {filepath}")
             # Optionally delete empty files immediately
             # try:
             #     os.remove(filepath)
             #     logger.info(f"🗑️  Deleted local empty file: {filepath}")
             # except Exception as delete_error:
             #     logger.warning(f"⚠️ Could not delete empty file {filepath}: {delete_error}")
             return

    except FileNotFoundError:
        logger.warning(f"⚠️ File not found during upload attempt: {filepath}. Skipping.")
        return
    except Exception as e:
        logger.error(f"❌ Could not get size for {filepath}: {e}. Skipping.")
        return


    progress = UploadProgressLogger(filename, filesize)

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"☁️ Attempting upload {attempt}/{MAX_RETRIES}: {filepath} to s3://{BUCKET_NAME}/{key}")
        try:
            s3.upload_file(
                Filename=filepath,
                Bucket=BUCKET_NAME,
                Key=key,
                Callback=progress
            )
            logger.info(f"✅ Successfully Uploaded: {filepath} to s3://{BUCKET_NAME}/{key}")
            try:
                os.remove(filepath)
                logger.info(f"🗑️  Deleted local file: {filepath}")
            except Exception as delete_error:
                logger.warning(f"⚠️ Could not delete {filepath}: {delete_error}")
            break  # Exit retry loop after success

        except ClientError as e:
            # Log specific S3 client errors
            logger.error(f"❌ S3 ClientError during upload attempt {attempt} for {filepath}: {e}")
            if attempt == MAX_RETRIES:
                 logger.error(f"❌ Giving up on {filepath} after {MAX_RETRIES} failed upload attempts.")
            else:
                 # Optional: add a short delay before retrying
                 time.sleep(5) # Wait 5 seconds before next retry
        except Exception as e:
             # Catch other potential errors during upload
             logger.error(f"❌ Unexpected error during upload attempt {attempt} for {filepath}: {e}")
             if attempt == MAX_RETRIES:
                 logger.error(f"❌ Giving up on {filepath} after {MAX_RETRIES} failed upload attempts.")
             else:
                 time.sleep(5) # Wait 5 seconds before next retry


# === Main Loop ===
def main():
    logger.info(f"🚀 Starting S3 uploader. Watching directories: {WATCH_DIRS}")
    logger.info(f"   Scanning every {SCAN_INTERVAL} seconds for .gz files older than {FILE_AGE_HOURS} hours.")
    logger.info(f"   Uploading to s3://{BUCKET_NAME}/{S3_PREFIX}/...")

    while True:
        for watch_dir in WATCH_DIRS:
            if not os.path.isdir(watch_dir):
                logger.warning(f"⚠️ Skipping invalid or non-existent directory: {watch_dir}")
                continue

            logger.info(f"🔎 Scanning directory tree: {watch_dir}")

            try:
                # os.walk yields (dirpath, dirnames, filenames) for each directory in the tree
                for root, dirs, files in os.walk(watch_dir):
                    # Process files in the current directory (root)
                    for fname in files:
                        # Construct the full path to the file
                        full_path = os.path.join(root, fname)

                        # Check if it's a .gz file and meets the age criteria
                        if fname.endswith(".gz") and is_older_than(full_path, FILE_AGE_HOURS):
                            logger.info(f"⏳ Found file meeting criteria: {full_path}")
                            upload_file(full_path, watch_dir) # Pass the original watch_dir here

            except Exception as e:
                # Catch unexpected errors during the directory tree scan
                logger.error(f"🚨 Error scanning directory tree {watch_dir}: {e}")

        logger.info(f"😴 Scan complete. Sleeping for {SCAN_INTERVAL} seconds.")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    # Restore original streams before potential SystemExit if logger fails
    try:
        main()
    finally:
         sys.stdout = original_stdout
         sys.stderr = original_stderr
         logging.shutdown() # Close log file handler
