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


project_root = Path(__file__).resolve().parent.parent.parent
API_KEY = os.environ.get("API_KEY")

def sync_extract(locations: str):
    """Locations must be like London,UK|Paris,France|Tokyo,Japan"""
    url_base = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services"
    url = url_base + f"/timelinemulti?key={API_KEY}&locations={locations}&unitGroup=metric"
    response = requests.get(url)

    if response.status_code != 200:
        raise ValueError("La requête a échoué")
    return response.json()



async def async_extract(locations):
    """Locations must be like London,UK|Paris,France|Tokyo,Japan"""
    url_base = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services"
    url = url_base + f"/timelinemulti?key={API_KEY}&locations={locations}&unitGroup=metric"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                raise ValueError("La requête a échoué")
            return await response.json()


def sync_transform(json_extracted) -> pd.DataFrame:
    """structuration de la données et autres traitements"""
    # reconstitution des données

    data_json = []
    data_in_location = json_extracted["locations"]

    for dict in data_in_location:
        new_dict = {}
        new_dict["id"] = str(uuid4())
        new_dict["resolvedAddress"] = dict["resolvedAddress"]
        new_dict["address"] = dict["address"]
        new_dict["latitude"] = dict["latitude"]
        new_dict["longitude"] = dict["longitude"]
        for date_json_data in dict["days"]:
            new_dict.update(date_json_data)
            # add new_data to data
            data_json.append(new_dict)

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

    payload_list = [value for _, value in payload.items()]

    # test : 2 chunks
    payload_test_list = payload_list[:n_chunks]

    extract_tasks = [async_extract(loc) for loc in payload_test_list]
    extract_results = await asyncio.gather(*extract_tasks) # mais asyncio.run() sous une fonction synchrone # ensemble des

    transform_tasks = [async_transform(json_extracted) for json_extracted in extract_results]
    transform_results = await asyncio.gather(*transform_tasks)
    print(payload_test_list)
    data = pd.concat(transform_results)
    data["department"] = data["resolvedAddress"].apply(choose_first)

    # definir les types de données
    for col in data.columns:
        if data[col].dtype == "object":
            data[col] = data[col].astype("string")

    return data


def load(data):
    """Load to Minio then to clickhouse"""
    # récupération du dataframe

    try:
        minio_storage = MinioUtils(bucket_name="weather")
        if data is not None:
         # load data to minio
            minio_storage.storage_df_to_parquet(df=data, prefix="raw/weatherdata/")
         # load data to clickhouse
        clickhouse_queries = ClickHouseQueries()
        clickhouse_queries.load_data_to_clickhouse(table_name="raw_weather_", data=data, is_to_truncate=True)
    except Exception as e:
        print(f"Il y a un problème : {e}")


if __name__ == "__main__":
    print(project_root)
    print(API_KEY)
    request_result = sync_extract("London,UK|Paris,France")
    print(request_result)
    with open(project_root / "etl.json", "w") as file:
        json.dump(request_result, file, indent=4)
