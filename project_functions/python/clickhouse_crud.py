
import logging
import textwrap # for formatting SQL queries
import pandas as pd
from pathlib import Path
from python.clickhouse_client import ClickHouseClient


# Configuration for connecting to ClickHouse
def get_clickhouse_client() -> ClickHouseClient:
    """
    Returns a ClickHouse client configured with the environment variables.
    """
    # Keep the logging level for ClickHouse and Airflow quiet to avoid cluttering the output
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING) # LOG FROM SQLALCHEMY
    logging.getLogger("airflow").setLevel(logging.WARNING) # LOG FROM AIRFLOW

    clickhouse_client = None
    try:
        print("Trying ClickHouseClient...")
        clickhouse_client = ClickHouseClient()
        print("Using ClickHouseClient.")
        return clickhouse_client  # Returns the ClickHouse client
    except Exception as ex:
        print(f"ClickHouseClient also failed: {ex}")
        raise RuntimeError("Failed to initialize any ClickHouse client.")

# create table and schema in ClickHouse
class ClickHouseQueries:
    def __init__(self):
        self.clickhouse_client = get_clickhouse_client()

    def ensure_table_exists(self, table_name: str) -> None:
        """Create the expected schema when a project table is missing in ClickHouse."""
        client = self.clickhouse_client
        if not client:
            raise ValueError("ClickHouse client is not initialized.")

        exists = client.run_query(f"EXISTS TABLE {table_name}").loc[0, "result"]
        if exists != 0:
            print(f"Table {table_name} exists in ClickHouse.")
            return

        schema_sql = {
            "raw_depcode": textwrap.dedent("""
                CREATE TABLE IF NOT EXISTS raw_depcode (
                    geo_point_2d String,
                    geo_shape String,
                    reg_name String,
                    reg_code String,
                    dep_name_upper String,
                    dep_current_code String,
                    dep_status Nullable(String)
                )
                ENGINE = MergeTree()
                ORDER BY dep_current_code
            """).strip(),
            "raw_depcode_": textwrap.dedent("""
                CREATE TABLE IF NOT EXISTS raw_depcode_ (
                    geo_point_2d String,
                    geo_shape String,
                    reg_name String,
                    reg_code String,
                    dep_name_upper String,
                    dep_current_code String,
                    dep_status Nullable(String),
                    department String,
                    dep_normalized String
                )
                ENGINE = MergeTree()
                ORDER BY dep_current_code
            """).strip(),
        }.get(table_name)

        if not schema_sql:
            raise ValueError(f"No schema is defined for ClickHouse table {table_name}.")

        print(f"Table {table_name} does not exist; creating it now...")
        client.get_conn().command(schema_sql)
        print(f"Table {table_name} created in ClickHouse.")

    def load_data_to_clickhouse(self, table_name: str, data: pd.DataFrame, is_to_truncate: bool=False) -> None:
        """
        Load data into ClickHouse table.
        raw_weather table has columns 'preciptype', 'stations' contain lists, but ClickHouse does not support list type.
        Therefore, we convert these columns to string before loading.
        if raw_weather table is to be created, set check_if_exists to True otherwise False.

        Parameters:
        -------------
            data : geojson data should have been handled
        """
        try:
            if data.empty:
                raise ValueError("Data to be loaded is empty.")
            client = self.clickhouse_client
            if not client:
                raise ValueError("ClickHouse client is not initialized.")
        except ValueError as ve:
            print(f"Error: {ve}")

        self.ensure_table_exists(table_name)

        # Ensure the ClickHouse client is initialized
        try:
            print(f"Loading data into ClickHouse table {table_name}...")
            # be ensure geojson data were transform before
            if is_to_truncate:
                client.get_conn().command(f"TRUNCATE TABLE {table_name}")  # Truncate the table if required
                client.get_conn().insert_df(table=table_name, df=data)
            # Insert data into ClickHouse table
            else:
                client.get_conn().insert_df(table=table_name, df=data)
            print(f"Data loaded into ClickHouse table {table_name} successfully.")
        except Exception as e:
            print(f"Error loading data to ClickHouse: {e}")

    def ensure_reference_tables_ready(self) -> None:
        """
        Daily guard: reference tables must exist before dbt starts.
        This is not a full bootstrap; it is a safe, idempotent precondition check.
        """
        client = self.clickhouse_client
        if not client:
            raise ValueError("ClickHouse client is not initialized.")

        required_tables = ["raw_depcode", "raw_depcode_"]
        for table_name in required_tables:
            self.ensure_table_exists(table_name)

            count = client.get_conn().query_df(query=f"SELECT count() AS cnt FROM {table_name}")
            rows = int(count.iloc[0].iloc[0])
            if rows == 0:
                raise ValueError(
                    f"Reference table {table_name} is missing data. "
                    "Run bootstrap_init_reference_tables or reload the reference data before dbt."
                )

        print("Reference tables are ready for dbt execution.")

    def bootstrap_init_reference_tables(self) -> None:
        """
        One-off initialization phase: ensure the reference tables exist and are populated.
        This is intended for cold starts, resets, or explicit bootstrap runs.
        """
        client = self.clickhouse_client
        if not client:
            raise ValueError("ClickHouse client is not initialized.")

        required_tables = ["raw_depcode", "raw_depcode_"]
        for table_name in required_tables:
            self.ensure_table_exists(table_name)

        for table_name in required_tables:
            count = client.get_conn().query_df(query=f"SELECT count() AS cnt FROM {table_name}")
            rows = int(count.iloc[0].iloc[0])
            if rows == 0:
                raise ValueError(f"Bootstrap failed: reference table {table_name} is empty.")

        print("Bootstrap reference tables are ready.")

    def merge_daily_data(self, table_name: str, target_table_name: str) -> None:
        """
        Append daily data to the ArchivedData table in ClickHouse.
        args:
            table_name: str - Name of the ClickHouse table to append data from.
            target_table_name: str - Name of the ClickHouse table to append data to
            (data: pd.DataFrame - DataFrame containing the data to be appended).
        """
        client = self.clickhouse_client
        if not client:
            raise ValueError("ClickHouse client is not initialized.")

        if client.get_conn().query_df(query=f"SELECT * FROM {table_name}").empty:
            raise ValueError(f"Data from Table {table_name} to be appended is empty.")

        print(f"Appending data from ClickHouse table {table_name} to table {target_table_name}...")
        query = f"""
            INSERT INTO {target_table_name}
            SELECT * FROM {table_name}
        """
        expected_query = textwrap.dedent(query).strip()
        try:
            client.get_conn().command(expected_query)
            print(f"Data appended to ClickHouse table {target_table_name} successfully.")
        except Exception as e:
            print(f"Error appending data to ClickHouse: {e}")
