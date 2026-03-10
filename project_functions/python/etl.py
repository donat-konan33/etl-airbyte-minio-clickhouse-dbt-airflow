from pathlib import Path
import requests
import asyncio
import json
import os
import pandas as pd
from uuid import uuid4
import gc
import io
import aiohttp
import asyncio
from python.minio_utils import choose_first, MinioUtils
from python.clickhouse_crud import ClickHouseQueries
import sys


project_root = Path(__file__).resolve().parent.parent.parent
API_KEY = os.environ.get("API_KEY")

sys.path.append(Path(__file__).resolve())

def sync_extract(locations: str):
    """Locations must be like London,UK|Paris,France|Tokyo,Japan"""
    url_base = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services"
    url = url_base + f"/timelinemulti?key={API_KEY}&locations={locations}&unitGroup=metric"
    response = requests.get(url)

    if response.status_code != 200:
        raise ValueError("La requête a échoué")
    return response.json()


async def async_extract(session, locations, sem):
    """Locations must be like London,UK|Paris,France|Tokyo,Japan"""

    async with sem: # goal: avoid too much connexion, otherwise crashing while extraction
        url_base = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services"
        url = url_base + f"/timelinemulti?key={API_KEY}&locations={locations}&unitGroup=metric"

        async with session.get(url) as response:
            if response.status != 200:
                raise ValueError("La requête a échoué")
            return await response.json()


def sync_transform(json_extracted) -> pd.DataFrame:
    """structuration de la données et autres traitements"""
    # reconstitution des données

    data_json = []
    data_in_location = json_extracted["locations"]

    for loc in data_in_location:
        new_dict = {}
        # get locations metadatas
        new_dict["id"] = str(uuid4())
        new_dict["resolvedAddress"] = loc["resolvedAddress"]
        new_dict["address"] = loc["address"]
        new_dict["latitude"] = loc["latitude"]
        new_dict["longitude"] = loc["longitude"]
        for date_json_data in loc["days"]:

            row = new_dict.copy() # change object reference, important because dict.update() changes object content inplace not the reference of object needed by .append()
            # get location days data day by day
            row.update(date_json_data)

            # add new_data to data
            data_json.append(row)

    data = pd.DataFrame(data_json)
    del data_json
    gc.collect()

    return data


async def async_transform(json_extracted):
    data = await asyncio.to_thread(sync_transform, json_extracted)
    return data


async def async_extract_transform(n_chunks: int=2):

    payload_path = project_root / "airbyte/france_departments_chunks.json"
    with open(str(payload_path), "r") as file:
        payload = json.load(file)

    payload_list = list(payload.values())

    # if slicing
    payload_list_sliced = payload_list[:n_chunks]

    # limit coroutines to 5
    connector = aiohttp.TCPConnector(limit=10)
    sem = asyncio.Semaphore(2)

    # get aiohttp session
    async with aiohttp.ClientSession(connector=connector) as session:
            extract_tasks = [async_extract(locations=loc, session=session, sem=sem) for loc in payload_list_sliced]
            extract_results = await asyncio.gather(*extract_tasks)

    transform_tasks = [async_transform(json_extracted) for json_extracted in extract_results]
    transform_results = await asyncio.gather(*transform_tasks)

    data = pd.concat(transform_results)
    data["department"] = data["resolvedAddress"].apply(choose_first)

    # definir les types de données
    for col in data.columns:
        if data[col].dtype == "object":
            data[col] = data[col].astype("string")

    return data


def extract_transform(n_chunks: int=2):
    """Extract transform function"""
    return asyncio.run(async_extract_transform(n_chunks=n_chunks))


def load(data):
    """Load to Minio then to clickhouse"""
    # récupération du dataframe

    try:
        minio_storage = MinioUtils(bucket_name="weather")

        if data is not None and not data.empty:

            # check out NA
            if data.isna().any().any():

                # columns with NA value
                cols = data.columns[data.isna().any()]

                # replace NA
                data[cols] = data[cols].replace({pd.NA: None})

            # load data to minio
            minio_storage.storage_df_to_parquet(df=data, prefix=["raw/weatherdata/",])

            # load data to clickhouse
            clickhouse_queries = ClickHouseQueries()
            clickhouse_queries.load_data_to_clickhouse(table_name="raw_weather_", data=data, is_to_truncate=True)
    except Exception as e:
        print(f"Existing an issue : {e}")


if __name__ == "__main__":

    # get max of chunks
    payload_path = project_root / "airbyte/france_departments_chunks.json"
    with open(str(payload_path), "r") as file:
        payload = json.load(file)
        n_chunks = len(payload)

    # step 1: Extract and transform weather data
    data = extract_transform(n_chunks=n_chunks)     # for tests n_chunks = 2, do not set n_chunks

    # step 2: load data to data lake(Minio) data warehouse(Clickhouse)
    load(data)
