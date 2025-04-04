import os
import pandas as pd
import argparse
from datetime import datetime
from tabulate import tabulate

def process_cur_files(directory):
    # Initialize an empty list to store DataFrames
    dataframes = []

    # Iterate through each subdirectory (month) in the specified directory
    for month_dir in os.listdir(directory):
        month_dir_path = os.path.join(directory, month_dir)
        if os.path.isdir(month_dir_path):  # Check if it's a directory
            # Iterate through each file in the month directory
            for filename in os.listdir(month_dir_path):
                if filename.endswith('.parquet'):  # Check if the file is a parquet file
                    file_path = os.path.join(month_dir_path, filename)
                    # Read the parquet file into a DataFrame
                    df = pd.read_parquet(file_path)
                    # Append the DataFrame to the list
                    dataframes.append(df)
    
    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)
    # Transpose the first few rows of the combined DataFrame
    transposed_df = combined_df.head().transpose()
    # Insert a column for row numbers
    transposed_df.insert(0, 'Row Number', range(1, 1 + len(transposed_df)))

    return transposed_df

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process CUR files.')
    # Add argument for directory with a default value
    parser.add_argument('-d', '--directory', type=str, default='sample/year=2018', help='Directory containing the CUR files')
    args = parser.parse_args()

    # Print the arguments for debugging purposes
    print(args)

    # Process the CUR files and get the transposed DataFrame
    cur_content = process_cur_files(args.directory)
    # Print the type of the resulting variable
    print(f"variable type is: " + str(type(cur_content)))

    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y-%m%d-%H%M')
    # Create an output filename using the timestamp
    output_filename = f'cur-sample-{timestamp}.txt'

    # Write the transposed DataFrame to a text file
    with open(output_filename, 'w') as f:
        # Create headers for the table
        headers = [f"{i}  {col}" for i, col in enumerate(cur_content.columns, start=1)]
        # Write the DataFrame to the file in a table format
        f.write(tabulate(cur_content, headers=headers, tablefmt='psql'))
    
    # Print a message indicating where the data was written
    print(f"data written to {output_filename}")
