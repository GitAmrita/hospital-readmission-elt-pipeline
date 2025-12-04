from fastapi import FastAPI
from fhir_api.fhir_api_helper import get_random_encounters_from_csv

app = FastAPI()


@app.get("/encounters")
def get_encounters(min_age: int = 0, max_age: int = 100, limit: int = 25):
    data = get_random_encounters_from_csv(min_age, max_age, limit)

    return {
        "resourceType": "Bundle",
        "type": "collection",
        "count": len(data),
        "entry": [{"resource": e} for e in data],
    }
