#!/bin/bash

# --- Configuration ---
CSV_FILE="your_projects.csv"  # <--- REPLACE with the actual path to your CSV file
SOURCE_BASE_PATH="/Users/manaspradhan/Desktop"
DESTINATION_BASE_PATH="/Users/manaspradhan/Desktop/dest-copy"
MAX_PROJECT_ID_FOR_COPY=9999 # Assuming we only care about projects below a certain ID (e.g., 9999)
# ---------------------

echo "Starting project copy script (v3)..."
echo "Reading file: $CSV_FILE"
echo "Condition: project_id is copied if it is <= $MAX_PROJECT_ID_FOR_COPY"
echo "Source Path Logic: Determined by 'project_id / 1000' (integer division)"

# Check if the CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    echo "ERROR: CSV file not found at $CSV_FILE"
    exit 1
fi

# Use awk to filter and format the matching lines.
# Fields: $1=username, $2=project_name, $3=project_id
# We'll filter for project IDs up to a reasonable limit, as the banding logic
# applies to all IDs.
awk -F',' 'NR > 1 && $3 <= '$MAX_PROJECT_ID_FOR_COPY' { print $1 " " $2 " " $3 }' "$CSV_FILE" |
while read USERNAME PROJECT_NAME PROJECT_ID; do
    
    # --- 1. Calculate the Source Directory Band ---
    # Bash uses double parentheses for arithmetic expansion. 
    # This performs integer division: 101/1000 = 0, 1001/1000 = 1, 2005/1000 = 2, etc.
    SOURCE_BAND=$(( PROJECT_ID / 1000 ))
    
    # --- 2. Define Source and Destination Paths ---
    # Source path is now based on the band
    SOURCE_DIR="$SOURCE_BASE_PATH/$SOURCE_BAND"
    
    # Destination path remains the same
    DEST_DIR="$DESTINATION_BASE_PATH/$USERNAME/$PROJECT_NAME/$PROJECT_ID"
    
    echo "--- Processing Match ---"
    echo "  User: $USERNAME, ID: $PROJECT_ID, Band: $SOURCE_BAND"
    echo "  Source: $SOURCE_DIR"
    echo "  Destination: $DEST_DIR"

    # Check if the calculated source directory (e.g., /abosultepath/0) exists
    if [ -d "$SOURCE_DIR" ]; then
        
        # Create the full destination directory path if it doesn't exist
        # This creates /destionationpath/<username>/<project_name>
        mkdir -p "$DESTINATION_BASE_PATH/$USERNAME/$PROJECT_NAME"

        # Use 'cp -r' to recursively copy the source directory (e.g., /abosultepath/0)
        # The new logic requires copying the *source band directory* into the project name folder.
        
        # We need to copy the *contents* of $SOURCE_DIR to the *newly created destination*.
        # The final folder should be named after $PROJECT_ID, but the source is the band.
        
        # CORRECTED LOGIC: Copy the Source Band directory ($SOURCE_DIR) 
        # but rename the final folder to $PROJECT_ID at the destination.
        
        # Copy the whole band directory and place it inside the Project Name folder, 
        # renaming it to the specific Project ID number.
        if cp -r "$SOURCE_DIR" "$DESTINATION_BASE_PATH/$USERNAME/$PROJECT_NAME/$PROJECT_ID"; then
            echo "  ✅ Copy Successful."
            echo "     Resulting path: $DESTINATION_BASE_PATH/$USERNAME/$PROJECT_NAME/$PROJECT_ID"
        else
            echo "  ❌ ERROR during copy operation."
        fi
    else
        echo "  WARNING: Source band directory not found: $SOURCE_DIR. Skipping."
    fi

done

echo ""
echo "Script finished."
