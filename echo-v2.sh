#!/bin/bash

# Define the name of your input file
INPUT_FILE="input.txt"

# 1. Check if the input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' not found."
    exit 1
fi

echo "---"
echo "Processing file: $INPUT_FILE"
echo "---"

# 2. Read the file line by line
while IFS= read -r number; do
    
    # Trim leading/trailing whitespace
    number=$(echo "$number" | xargs)
    
    # Skip empty lines
    if [[ -z "$number" ]]; then
        continue
    fi
    
    # 3. Apply the Conditions
    
    # Condition 1: number <= 999 
    if [[ "$number" -le 999 ]]; then
        DIR_CODE="0"
        
    # Condition 2: > 999 AND <= 1999 
    elif [[ "$number" -gt 999 ]] && [[ "$number" -le 1999 ]]; then
        DIR_CODE="1"
        
    # Condition 3: > 1999 AND <= 2999
    elif [[ "$number" -gt 1999 ]] && [[ "$number" -le 2999 ]]; then
        DIR_CODE="2"
        
    # Default case: number is outside all defined ranges
    else
        echo "Skipping $number: Outside the defined ranges (0-2999)."
        continue # Skip to the next number
    fi
    
    # 4. Construct and echo the desired path using the determined DIR_CODE
    echo "/hello/myname/$DIR_CODE/$number"
    
done < "$INPUT_FILE"

echo "---"
echo "✅ Finished processing."
