
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

        # create table if it does not exist with schema below
        schema_sql = {
            "raw_weather_": textwrap.dedent("""
                CREATE TABLE IF NOT EXISTS raw_weather_ (
                    id String,

                    resolvedAddress String,
                    address String,

                    latitude Float64,
                    longitude Float64,

                    datetime String,
                    datetimeEpoch Int64,

                    tempmax Float64,
                    tempmin Float64,
                    temp Float64,

                    feelslikemax Float64,
                    feelslikemin Float64,
                    feelslike Float64,

                    dew Float64,
                    humidity Float64,

                    precip Float64,
                    precipprob Float64,
                    precipcover Float64,
                    preciptype Nullable(String),

                    snow Float64,
                    snowdepth Float64,

                    windgust Float64,
                    windspeed Float64,
                    winddir Float64,

                    pressure Float64,
                    cloudcover Float64,
                    visibility Float64,

                    solarradiation Float64,
                    solarenergy Float64,
                    uvindex Float64,

                    severerisk Float64,

                    sunrise String,
                    sunriseEpoch Int64,

                    sunset String,
                    sunsetEpoch Int64,

                    moonphase Float64,

                    conditions String,
                    description String,
                    icon String,

                    stations Nullable(String),
                    source String,

                    department String
                )
                ENGINE = MergeTree
                ORDER BY (id, department, datetime)
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

        Args:
            table_name: str - Name of the ClickHouse table to load data into.
            data: pd.DataFrame - DataFrame containing the data to be loaded.
            is_to_truncate: bool - Whether to truncate the table before loading data.

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
            # be ensure geojson data were transformed before
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

        required_tables = ["raw_weather_", "raw_depcode_"]
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

        required_tables = ["raw_weather_", "raw_depcode_"]
        for table_name in required_tables:
            self.ensure_table_exists(table_name)

        # Load department reference data if raw_depcode_ is empty
        count = client.get_conn().query_df(query=f"SELECT count() AS cnt FROM raw_depcode_")
        rows = int(count.iloc[0].iloc[0])
        if rows == 0:
            print("raw_depcode_ is empty, loading reference department data...")
            self._load_department_reference_data()

        for table_name in required_tables:
            count = client.get_conn().query_df(query=f"SELECT count() AS cnt FROM {table_name}")
            rows = int(count.iloc[0].iloc[0])
            if rows == 0:
                raise ValueError(f"Bootstrap failed: reference table {table_name} is empty.")

        print("Bootstrap reference tables are ready.")

    def _load_department_reference_data(self) -> None:
        """
        Load fixed French department reference data (raw_depcode_).
        Merges department codes with geographic data (regions, coordinates, shapes).
        This is a one-time bootstrap operation for initialization.
        """
        import shapely.wkb
        from shapely import wkt
        import json
        from pathlib import Path
        from python.functions import TransformData

        project_root = Path(__file__).resolve().parent.parent.parent

        department_path = project_root / "data/location/departements_france_selection.csv"
        dep_geo_path = project_root / "data/location/france_region_department96.parquet"

        if not department_path.exists():
            raise FileNotFoundError(f"Department data file not found: {department_path}")
        if not dep_geo_path.exists():
            raise FileNotFoundError(f"Geographic data file not found: {dep_geo_path}")

        # Load department codes and geographic data
        department_data = pd.read_csv(department_path)
        dep_geo = pd.read_parquet(dep_geo_path)

        # Normalize department names for proper merging
        normalizer = TransformData()
        department_data["dep_normalized"] = normalizer.normalize(department_data["department"])

        # Merge on department code
        data = dep_geo.merge(department_data, on="dep_current_code", how="left")

        # Convert WKB to WKT format and handle GeoJSON conversion
        def wkb_to_wkt(x):
            try:
                return shapely.wkb.loads(x).wkt
            except Exception:
                return None

        if "geo_point_2d" in data.columns:
            data["geo_point_2d"] = data["geo_point_2d"].apply(wkb_to_wkt)
        if "geo_shape" in data.columns:
            data["geo_shape"] = data["geo_shape"].apply(wkb_to_wkt)
            data["geo_shape"] = data["geo_shape"].apply(
                lambda w: wkt.loads(w).__geo_interface__ if w else None
            )
            data["geo_shape"] = data["geo_shape"].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else x
            )

        # Ensure all string columns are properly typed
        for col in ["reg_name", "reg_code", "dep_name_upper", "dep_current_code", "dep_status"]:
            if col in data.columns:
                data[col] = data[col].apply(lambda x: str(x) if not pd.isna(x) else x)

        for col in data.select_dtypes(include=["object"]).columns:
            data[col] = data[col].astype("string")

        # Load into ClickHouse
        print(f"Loading {len(data)} department reference records into raw_depcode_...")
        self.load_data_to_clickhouse(
            table_name="raw_depcode_",
            data=data,
            is_to_truncate=True
        )

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
