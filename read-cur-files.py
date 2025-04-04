import os
import pandas as pd
import argparse
from datetime import datetime
from tabulate import tabulate

def process_cur_files(directory):

    dataframes = []

    for month_dir in os.listdir(directory):
        month_dir_path = os.path.join(directory, month_dir)
        if os.path.isdir(month_dir_path):
            for filename in os.listdir(month_dir_path):
                if filename.endswith('.parquet'):
                    file_path = os.path.join(month_dir_path, filename)
                    df = pd.read_parquet(file_path)
                    dataframes.append(df)
    combined_df = pd.concat(dataframes, ignore_index=True)
    transposed_df = combined_df.head().transpose()
    transposed_df.insert(0, 'Row Number', range(1, 1 + len(transposed_df)))

    return transposed_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process CUR files.')
    parser.add_argument('-d', '--directory', type=str, default='sample/year=2018', help='Directory containing the CUR files')
    args = parser.parse_args()
    print(args)

    cur_content = process_cur_files(args.directory)
    print(f"variable type is: " + str(type(cur_content)))

    timestamp = datetime.now().strftime('%Y-%m%d-%H%M')
    output_filename = f'cur-sample-{timestamp}.txt'

    with open(output_filename, 'w') as f:
        headers = [f"{i}  {col}" for i, col in enumerate(cur_content.columns, start=1)]
        f.write(tabulate(cur_content, headers=headers, tablefmt='psql'))
    print(f"data written to {output_filename}")
