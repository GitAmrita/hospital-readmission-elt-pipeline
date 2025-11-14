# Hospital Readmission ELT Pipeline

This project demonstrates an end-to-end **ELT pipeline** for predicting hospital readmissions using synthetic EHR data. It includes:

- Batch CSV ingestion (file drop runs every 10 minutes to check for new files)
- API-based ingestion
- Streaming ingestion simulation
- Transform layers (Bronze → Silver → Gold) for ML-ready features
- Readmission prediction using machine learning models

Datasource: https://archive.ics.uci.edu/dataset/296/diabetes+130-us+hospitals+for+years+1999-2008

