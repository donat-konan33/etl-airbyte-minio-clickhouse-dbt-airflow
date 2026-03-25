from python.clickhouse_crud import get_clickhouse_client
import pandas as pd
from typing import Optional


# create table and schema in ClickHouse
class ClickHouseQuery:
    def __init__(self):
        self.clickhouse_client = get_clickhouse_client()

    def load_data_to_clickhouse(self, database : str, table_name: str, data: pd.DataFrame, check_if_exists: Optional[bool] = True) -> None:
        """
        Load data into ClickHouse table.
        raw_weather table has columns 'preciptype', 'stations' contain lists, but ClickHouse does not support list type.
        Therefore, we convert these columns to string before loading.
        if raw_weather table is to be created, set check_if_exists to True otherwise False.
        """
        try:
            client = self.clickhouse_client
            if not client:
                raise ValueError("ClickHouse client is not initialized.")
            if data.empty:
                raise ValueError("Data to be loaded is empty.")
            else:
                print(f"Loading data into ClickHouse table {table_name}...")
                if check_if_exists:
                    # Check if the table exists
                    if client.run_query(
                        f"""EXISTS TABLE {database}.{table_name}"""
                    ).loc[0, "result"] == 0:
                        raise ValueError(f"Table {table_name} does not exist in ClickHouse.")
                    print(f"Target Table {table_name} exists in ClickHouse, loading be continue")
                # convert preciptype and stations columns because clickhouse does not support list type
                    for col in ['preciptype', 'stations']:
                        if col in data.columns:
                            data[col] = data[col].apply(lambda x: str(x) if not pd.isna(x) else x)
                else :
                    data["dep_status"] = data["dep_status"].astype("string")
                # Insert data into ClickHouse table
                client.get_conn().insert_df(table_name, df=data)
                print(f"Data loaded into ClickHouse table {table_name} successfully.")
        except Exception as e:
            print(f"Error loading data to ClickHouse: {e}")
