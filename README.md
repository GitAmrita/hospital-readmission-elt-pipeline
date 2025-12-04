# Hospital Readmission ELT Pipeline

This project demonstrates an end-to-end **ELT pipeline** for predicting hospital readmissions using synthetic EHR data. It includes:

- Batch CSV ingestion (file drop runs every 10 minutes to check for new files)
- API-based ingestion
- Streaming ingestion simulation
- Transform layers (Bronze → Silver → Gold) for ML-ready features
- Readmission prediction using machine learning models

Datasource: https://archive.ics.uci.edu/dataset/296/diabetes+130-us+hospitals+for+years+1999-2008

Api integration 
Overview

Simulates FHIR API ingestion for hospital encounter data. Reads patient encounters from a CSV filtered by age(incoming/hospital_data.csv) and inserts them into the DuckDB raw table raw.all_encounters with:

source = "api" and ingestion_time = current timestamp

Endpoints - Returns random patient encounters.
GET /encounters
Query parameters: min_age, max_age, limit

Usage
Start local API and ingestion
- python run_local_api_ingestion.py
    API at http://127.0.0.1:8000
    Fetches random encounters from CSV and inserts into DB
- Batch ingestion via main.py. python main.py
    Fetches encounters once and exits (API runs temporarily)


File ingestion
Overview

Simulates batch file-drop ingestion into DuckDB. Watches an incoming folder for a CSV, samples rows, and writes them into the raw table `raw.all_encounters` with:

- source = "file"
- ingestion_time = current timestamp

Sample usage

- Run the file-drop watcher:
  - `python -m ingestions.file_ingestion` (or call `watch_for_file_drop()` from a small runner script)
- Drop `hospital_data.csv` into `raw_data/incoming`.
- After up to 10 minutes, the file is detected, sampled, loaded into DuckDB, and moved to `raw_data/processed`.
- Run it from main.py alternatively.

