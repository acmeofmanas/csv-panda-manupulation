#!/bin/bash
# Process each numbered directory in parallel
for dir in /base/path/*/; do
  (
    dir_name=$(basename "$dir")
    echo "Processing $dir_name"
    aws s3 sync "$dir" s3://your-bucket/"$dir_name"/ \
      --storage-class INTELLIGENT_TIERING \
      --exclude "*.tmp" \
      --exclude "*.log"
  ) &
  
  # Limit concurrent directory syncs
  if [[ $(jobs -r -p | wc -l) -ge 5 ]]; then
    wait -n
  fi
done
wait
