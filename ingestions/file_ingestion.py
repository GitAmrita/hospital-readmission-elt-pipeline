import os
import pandas as pd
import shutil
import time
from database.db import get_connection, RAW_TABLE, create_raw_schema, create_raw_table_if_not_exists

def load_csv_batch(file_path):
    con = get_connection()
    create_raw_schema(con)

    # Read CSV
    df = pd.read_csv(file_path)

    # Take 50% random sample
    df_sample = df.sample(frac=0.5, random_state=42).reset_index(drop=True)

    # Add metadata columns
    df_sample['source'] = 'file'
    df_sample['ingestion_time'] = pd.Timestamp.now()

    # Create table if not exists
    create_raw_table_if_not_exists(con, df_sample)

    # Insert sampled rows
    con.execute(f"INSERT INTO {RAW_TABLE} SELECT * FROM df_sample")
    print(f"Loaded {len(df_sample)} rows from {file_path} into {RAW_TABLE}")

def watch_for_file_drop(incoming_dir="raw_data/incoming", processed_dir="raw_data/processed", file_name="hospital_data.csv"):
    print("Watching for file drops...")
    while True:
        files = [f for f in os.listdir(incoming_dir) if f.endswith(".csv")]
        if file_name in files:
            file_path = os.path.join(incoming_dir, file_name)
            load_csv_batch(file_path)

            # Move file to processed
            os.makedirs(processed_dir, exist_ok=True)
            shutil.move(file_path, os.path.join(processed_dir, file_name))
            print(f"Processed file {file_name} and moved to processed folder")

        
        time.sleep(60 * 10) # 10 minutes
