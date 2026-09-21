import numpy as np
import pandas as pd

from project_functions.python.etl import sync_transform
from project_functions.python.minio_utils import choose_first


def test_choose_first_handles_missing_values() -> None:
    values = pd.Series(["Paris, France", None, np.nan, "Lyon, France"])

    result = choose_first(values)

    assert result.tolist() == ["Paris", None, None, "Lyon"]


def test_sync_transform_expands_location_days_into_rows() -> None:
    payload = {
        "locations": [
            {
                "resolvedAddress": "Paris, France",
                "address": "Paris",
                "latitude": 48.8566,
                "longitude": 2.3522,
                "days": [
                    {"datetime": "2024-01-01", "temp": 10.0, "conditions": "Clear"},
                    {"datetime": "2024-01-02", "temp": 11.5, "conditions": "Cloudy"},
                ],
            }
        ]
    }

    df = sync_transform(payload)

    assert len(df) == 2
    assert list(df.columns)[:6] == ["id", "resolvedAddress", "address", "latitude", "longitude", "datetime"]
    assert df["address"].tolist() == ["Paris", "Paris"]
    assert df["temp"].tolist() == [10.0, 11.5]
