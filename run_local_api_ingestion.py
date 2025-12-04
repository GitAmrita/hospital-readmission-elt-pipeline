import threading
import time
import uvicorn
from ingestions.api_ingestion import fetch_and_insert_encounters
import fhir_api.fhir_api_endpoints as fhir_api_endpoints  # your FastAPI file


def start_local_api_and_ingestion(min_age=30, max_age=70, limit=50, run_forever=True):
    """
    Starts FastAPI locally and runs API ingestion.
    
    Args:
        min_age: minimum age for encounters
        max_age: maximum age for encounters
        limit: number of encounters to fetch
        run_forever: if True, keeps the API running; if False, exits after ingestion
    """
    # Start FastAPI in a separate thread
    api_thread = threading.Thread(
        target=lambda: uvicorn.run(
            fhir_api_endpoints.app,
            host="127.0.0.1",
            port=8000,
            log_level="info"
        ),
        daemon=True
    )
    api_thread.start()

    # Wait a few seconds for the API to be ready
    time.sleep(3)

    # Fetch from API and insert into DuckDB
    print("Starting API ingestion...")
    fetch_and_insert_encounters(min_age=min_age, max_age=max_age, limit=limit)
    print(f"Ingestion complete. API running at http://127.0.0.1:8000")

    # Keep the API running if requested
    if run_forever:
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping local API and ingestion.")
