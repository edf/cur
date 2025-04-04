import os
import pandas as pd
import argparse

def process_cur_files(directory):
    # List all files in the directory
    files = [filename for filename in os.listdir(directory) if os.path.isfile(os.path.join(directory, filename))]
    
    for file in files:
        file_path = os.path.join(directory, file)
        # Read the CUR file into a DataFrame
        df = pd.read_csv(file_path)
        # Transpose the DataFrame
        df_transposed = df.transpose()
        # Print the column headers
        print(f"Column headers for {file}: {list(df_transposed.columns)}")
        # Number the rows
        df_transposed.index = range(1, len(df_transposed) + 1)
        # Print the DataFrame with numbered rows
        print(f"Transposed DataFrame for {file}:\n{df_transposed}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process CUR files.')
    parser.add_argument('-d', '--directory', type=str, default='sample/year=2018', help='Directory containing the CUR files')
    args = parser.parse_args()
    
    process_cur_files(args.directory)
