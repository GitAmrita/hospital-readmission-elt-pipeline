import pandas as pd
import numpy as np
import random
from pathlib import Path

INCOMING_CSV = Path("raw_data/incoming/hospital_data.csv")


def parse_age_bucket(age_bucket: str):
    """
    Convert an age bucket like '[50-60)' into (50, 60)
    """
    clean = age_bucket.strip("[]()")
    low, high = clean.split("-")
    return int(low), int(high)


def get_random_encounters_from_csv(min_age: int = 0, max_age: int = 100, limit: int = 25):
    """
    Reads encounters directly from the CSV file, filters by age bucket,
    and samples random rows.
    Returns a list of dictionaries.
    """
    if not INCOMING_CSV.exists():
        raise FileNotFoundError(f"{INCOMING_CSV} does not exist")

    df = pd.read_csv(INCOMING_CSV)

    # Filter by age bucket
    def age_in_range(bucket: str) -> bool:
        """Check if age bucket overlaps with the specified range."""
        low, high = parse_age_bucket(bucket)
        return low >= min_age and high <= (max_age + 1)

    filtered = df[df["age"].apply(age_in_range)]

    if filtered.empty:
        return []

    # Sample random rows
    sample = filtered.sample(n=min(limit, len(filtered)))

    # Clean non-JSON-safe float values (NaN, inf, -inf) so FastAPI can serialize to JSON
    sample = sample.replace([np.inf, -np.inf], np.nan)
    sample = sample.where(pd.notna(sample), None)

    # Convert to list of dicts
    encounters = sample.to_dict(orient="records")

    return encounters
