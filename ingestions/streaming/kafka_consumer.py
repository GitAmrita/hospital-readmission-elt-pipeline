import json
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer

from database.db import RAW_TABLE, get_connection

consumer = KafkaConsumer(
    "encounters",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="encounter_ingestion_group",
)


def consume_and_insert():
    conn = get_connection()

    for msg in consumer:
        encounter = msg.value
        encounter["source"] = "kafka"
        encounter["ingestion_time"] = datetime.now()
        df = pd.DataFrame([encounter])

        conn.register("temp_df", df)
        conn.execute(f"INSERT INTO {RAW_TABLE} SELECT * FROM temp_df")
        conn.unregister("temp_df")

        print("Inserted encounter:", encounter.get("encounter_id"))
