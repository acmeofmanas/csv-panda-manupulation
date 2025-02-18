import pandas as pd
import glob
import os
from datetime import datetime

def process_csv_files(input_directory, output_directory, columns_to_keep):
    """
    Process multiple CSV files in a directory, keeping only specified columns.
    
    Parameters:
    input_directory (str): Path to directory containing CSV files
    output_directory (str): Path to directory for processed files
    columns_to_keep (list): List of column names to retain
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print(f"Created output directory: {output_directory}")

    # Get list of all CSV files
    csv_files = glob.glob(os.path.join(input_directory, "*.csv"))
    total_files = len(csv_files)
    print(f"\nFound {total_files} CSV files to process")
    
    # Initialize counters
    processed_files = 0
    total_rows_processed = 0
    start_time = datetime.now()
    
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        file_start_time = datetime.now()
        
        print(f"\nProcessing file {processed_files + 1}/{total_files}: {file_name}")
        
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            initial_rows = len(df)
            initial_columns = len(df.columns)
            
            print(f"Initial shape: {df.shape}")
            
            # Verify columns exist
            available_columns = [col for col in columns_to_keep if col in df.columns]
            missing_columns = [col for col in columns_to_keep if col not in df.columns]
            
            if missing_columns:
                print(f"Warning: Missing columns in {file_name}: {missing_columns}")
            
            # Keep only specified columns
            df = df[available_columns]
            
            # Remove rows with all NaN values
            df = df.dropna(how='all')
            final_rows = len(df)
            
            # Save processed file
            output_path = os.path.join(output_directory, f"processed_{file_name}")
            df.to_csv(output_path, index=False)
            
            # Update counters
            processed_files += 1
            total_rows_processed += final_rows
            
            # Calculate and display file statistics
            processing_time = (datetime.now() - file_start_time).total_seconds()
            rows_removed = initial_rows - final_rows
            columns_removed = initial_columns - len(available_columns)
            
            print(f"Rows removed: {rows_removed}")
            print(f"Columns removed: {columns_removed}")
            print(f"Final shape: {df.shape}")
            print(f"Processing time: {processing_time:.2f} seconds")
            
        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")
    
    # Calculate and display final statistics
    total_time = (datetime.now() - start_time).total_seconds()
    
    print(f"\nProcessing Complete!")
    print(f"Total files processed: {processed_files}/{total_files}")
    print(f"Total rows processed: {total_rows_processed}")
    print(f"Total processing time: {total_time:.2f} seconds")
    print(f"Average time per file: {(total_time/processed_files):.2f} seconds")

# Example usage
if __name__ == "__main__":
    # Configuration
    input_dir = "/Users/manaspradhan/Downloads/dir-csv/"
    output_dir = "/Users/manaspradhan/Downloads/processed_csvs/"
    columns_to_keep = ["id", "user", "email"]  # Replace with your column names
    
    # Process files
    process_csv_files(input_dir, output_dir, columns_to_keep)
