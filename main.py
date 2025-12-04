from run_local_api_ingestion import start_local_api_and_ingestion
from ingestions.file_ingestion import watch_for_file_drop

if __name__ == "__main__":
    # Start watching for batch CSV file drops
    # watch_for_file_drop()

   # Call the local API + ingestion pipeline
    start_local_api_and_ingestion(min_age=30, max_age=70, limit=50, run_forever=False)
