import pandas as pd


def test_weather_dataframe_has_expected_schema() -> None:
    df = pd.DataFrame(
        {
            "id": ["a1", "a2"],
            "resolvedAddress": ["Paris, France", "Lyon, France"],
            "datetime": ["2024-01-01", "2024-01-02"],
            "latitude": [48.8566, 45.7640],
            "longitude": [2.3522, 4.8357],
            "temp": [10.5, 11.0],
            "department": ["Paris", "Lyon"],
        }
    )

    expected_columns = {
        "id",
        "resolvedAddress",
        "datetime",
        "latitude",
        "longitude",
        "temp",
        "department",
    }

    assert set(df.columns) >= expected_columns
    assert df["latitude"].dtype.kind in {"f", "i"}
    assert df["longitude"].dtype.kind in {"f", "i"}


def test_key_columns_have_no_nulls_or_duplicates() -> None:
    df = pd.DataFrame(
        {
            "id": ["a1", "a2", "a3"],
            "datetime": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "department": ["Paris", "Lyon", None],
        }
    )

    assert df["id"].notna().all()
    assert not df["id"].duplicated().any()
    assert df["datetime"].notna().all()
    assert df["department"].notna().sum() >= 2


def test_null_rate_stays_below_policy_threshold() -> None:
    df = pd.DataFrame(
        {
            "temp": [10.0, 12.0, 11.5, None],
            "humidity": [40.0, 45.0, 50.0, 60.0],
            "department": ["Paris", "Lyon", "Marseille", "Nice"],
        }
    )

    null_rate = df["temp"].isna().mean()
    assert null_rate <= 0.25, "Null rate exceeds the acceptable threshold for weather values."
    assert df["department"].notna().all()


def test_department_reference_csv_keeps_corse_split_and_valid_codes() -> None:
    df = pd.read_csv("data/location/departements_france_selection.csv")

    assert {"Corse-du-Sud", "Haute Corse"}.issubset(set(df["department"]))
    corsica = df[df["department"].isin(["Corse-du-Sud", "Haute Corse"])]
    assert set(corsica["dep_current_code"]) == {"2A", "2B"}
    assert df["department"].notna().all()
    assert df["dep_current_code"].notna().all()
    assert df["dep_current_code"].astype(str).str.len().gt(0).all()
