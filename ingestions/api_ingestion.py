import requests
from datetime import datetime
import pandas as pd

from database.db import (
    RAW_TABLE,
    create_raw_schema,
    create_raw_table_if_not_exists,
    get_connection,
)

API_URL = "http://127.0.0.1:8000/encounters"
# "http://localhost:8000/encounters"


def fetch_encounters_from_api(min_age: int = 0, max_age: int = 100, limit: int = 25):
    """
    Fetch random encounters from the API and return as a list of dictionaries.
    """
    params = {"min_age": min_age, "max_age": max_age, "limit": limit}
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    bundle = response.json()
    encounters = [entry["resource"] for entry in bundle.get("entry", [])]
    return encounters


def insert_encounters_into_db(encounters):
    """
    Insert list of encounters into raw.all_encounters, adding source and ingestion_time.
    """
    if not encounters:
        print("No encounters to insert.")
        return

    now_ts = datetime.now()
    for e in encounters:
        e["source"] = "api"
        e["ingestion_time"] = now_ts

    df = pd.DataFrame(encounters)

    con = get_connection()
    create_raw_schema(con)
    create_raw_table_if_not_exists(con, df)

    con.register("temp_df", df)
    con.execute(f"INSERT INTO {RAW_TABLE} SELECT * FROM temp_df")
    con.unregister("temp_df")
    con.close()

    print(f"Inserted {len(encounters)} encounters into raw.all_encounters")


def fetch_and_insert_encounters(min_age: int = 0, max_age: int = 100, limit: int = 25):
    """
    Calls fetch_encounters_from_api and then inserts into the database.
    """
    encounters = fetch_encounters_from_api(min_age, max_age, limit)
    insert_encounters_into_db(encounters)


# if __name__ == "__main__":
#     # Example: fetch 50 encounters for age 30–70
#     fetch_and_insert_encounters(min_age=30, max_age=70, limit=50)
