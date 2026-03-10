#! /usr/bin/env python

import sys
from pathlib import Path
project_root = Path().resolve()

# add project_root to python
sys.path.append(str(project_root))

import pandas as pd
import shapely.wkb
from shapely import wkt
import json

from project_functions.python.clickhouse_crud import ClickHouseQueries
from project_functions.python.functions import TransformData


def wkb_to_wkt(x):
    try:
        return shapely.wkb.loads(x).wkt
    except Exception:
        return None


department_path = project_root / "data/location/departements_france_selection.csv"
dep_geo_path = project_root / "data/location/france_region_department96.parquet"

def main():
    department_data = pd.read_csv(department_path)

    # get department normalizer
    normalizer = TransformData()
    department_data["dep_normalized"] = normalizer.normalize(department_data["department"]) # useful when merging depcode table in data extracted on department name, that allows us to merge effectively

    # merge data
    dep_geo = pd.read_parquet(dep_geo_path)
    data = dep_geo.merge(department_data, on="dep_current_code", how="left")

    if data.columns.isin(["geo_point_2d", "geo_shape"]).any(): # check whether at least one of the columns is in the DataFrame.columns Index
        # Need to convert data types for ClickHouse compatibility # transform type according to the target table in clickhousedb before loading it to.
        # for check_if_exists equals False, only if depcode table is input as data argument
        data["geo_point_2d"] = data["geo_point_2d"].apply(wkb_to_wkt)
        data["geo_shape"] = data["geo_shape"].apply(wkb_to_wkt)
        data["geo_shape"] = data["geo_shape"].apply(lambda w: wkt.loads(w).__geo_interface__ if w else None)  # Convert to GeoJSON format
        data["geo_shape"] = data["geo_shape"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)


    for col in ["reg_name", "reg_code", "dep_name_upper", "dep_current_code", "dep_status"]:
        data[col] = data[col].apply(lambda x: str(x) if not pd.isna(x) else x)

    # adjust datas types to match with the target table in database (set to string)
    for i in data.select_dtypes(include=["object",]).columns:
        data[i] = data[i].astype("string")

    # load data to the database on the table raw_depcode
    df_clickhouse_loader = ClickHouseQueries()
    df_clickhouse_loader.load_data_to_clickhouse(
        table_name="raw_depcode_",
        data=data,
        is_to_truncate=False
)

if __name__ == "__main__":
    main()
