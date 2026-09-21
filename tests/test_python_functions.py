import pytest
import pandas as pd
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from project_functions.python.clickhouse_crud import ClickHouseQueries, get_clickhouse_client
from weather_api.main import app
import textwrap
# fixtures for the tests : prepare resources for the tests

client = TestClient(app)

@pytest.fixture
def load_parquet_df():
    """
    retrieve data from real parquet file
    """
    def _load_parquet_df(path):
       return pd.read_parquet(path)
    return _load_parquet_df

def test_health_endpoint_returns_ok():
    """Health endpoint should be available for deployment checks."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_clickhouse_client_falls_back_to_localhost_when_docker_host_is_unreachable():
    """Host-side scripts should retry localhost when Docker service names are not resolvable."""
    from project_functions.python.clickhouse_client import ClickHouseClient

    client_instance = ClickHouseClient()
    client_instance.params = {
        "hostip": "clickhouse-server",
        "hostname": "clickhouse-server",
        "port": 8123,
        "username": "user",
        "password": "pass",
        "database": "default",
    }

    attempts = []

    def fake_get_client(*, host, port, username, password, database):
        attempts.append(host)
        if host == "clickhouse-server":
            raise ConnectionError("cannot resolve docker host")

        class DummyClient:
            def command(self, query):
                assert query == "SELECT 1"
                return "1"

        return DummyClient()

    with patch("project_functions.python.clickhouse_client.clickhouse_connect.get_client", side_effect=fake_get_client):
        conn = client_instance.get_conn()

    assert conn is not None
    assert "clickhouse-server" in attempts
    assert "localhost" in attempts


@pytest.mark.parametrize("table_exists, should_raise",[
                        (1, False), # Table exists -> no raise
                        (0, True) # Table does not exist -> should raise ValueError
    ]
)
@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_table_exists(mock_clickhouse_client, table_exists, should_raise):
    """
    Test if the ClickHouse table exists.
    """
    mock_client = MagicMock()
    mock_conn = MagicMock()
    # mock config
    mock_client.get_conn.return_value = mock_conn
    mock_client.run_query.return_value = pd.DataFrame([[table_exists]], columns=["result", ])
    mock_clickhouse_client.return_value = mock_client

    clickhouse_queries = ClickHouseQueries()
    if should_raise:
        with pytest.raises(ValueError, match="No schema is defined for ClickHouse table raw_weather."):
            clickhouse_queries.load_data_to_clickhouse(table_name="raw_weather", data=pd.DataFrame({"column1": [1, 2, 3]}))
    else:
        clickhouse_queries.load_data_to_clickhouse(table_name="raw_weather", data=pd.DataFrame({"column1": [1, 2, 3]}))
        mock_client.run_query.assert_called_once_with("EXISTS TABLE raw_weather")


@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_missing_project_table_is_created_before_insert(mock_clickhouse_client):
    """Project tables should be created automatically when the schema is known."""
    mock_client = MagicMock()
    mock_conn = MagicMock()
    mock_client.get_conn.return_value = mock_conn
    mock_client.run_query.return_value = pd.DataFrame([[0]], columns=["result"])
    mock_clickhouse_client.return_value = mock_client

    clickhouse_queries = ClickHouseQueries()
    clickhouse_queries.load_data_to_clickhouse(table_name="raw_depcode_", data=pd.DataFrame({"column1": [1, 2, 3]}))

    mock_conn.command.assert_any_call(
        """
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
""".strip()
    )


@pytest.mark.parametrize("path, is_to_truncate, table_name",
                         [("tests/data/france_weather_2025-04-13.parquet", True, "raw_weather"),
                          ("tests/data/france_region_department96.parquet", False, "raw_depcode")]
)
@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_load_data_to_clickhouse(mock_clickhouse_client, load_parquet_df, path, is_to_truncate, table_name):
    """
     Test load_data_to_clickhouse with multiple .parquet files and truncation options.
    """
    mock_client = MagicMock()
    mock_conn = MagicMock()
    # mock config
    mock_client.get_conn.return_value = mock_conn
    mock_client.run_query.return_value = pd.DataFrame([[1]], columns=["result", ]) # Simulate a response indicating the table exists, specifically for the check if table exists we get a DataFrame with a single value of 1 and column named "result"
    mock_clickhouse_client.return_value = mock_client

    # test different method of client
    clickhouse_queries = ClickHouseQueries()
    df = load_parquet_df(path)
    clickhouse_queries.load_data_to_clickhouse(table_name=table_name, data=df, is_to_truncate=is_to_truncate)

    # Check that the client was called with the correct parameters, after simulating loading data
    mock_client.run_query.assert_called_once_with(f"EXISTS TABLE {table_name}")

    if df.columns.isin(["geo_point_2d", "geo_shape"]).any():
        # Check that the conversion to string was applied to the appropriate columns
        assert list(df.columns) == ['geo_point_2d',
                              'geo_shape',
                            'reg_name',
                            'reg_code',
                            'dep_name_upper',
                            'dep_current_code',
                            'dep_status'
                            ], "DataFrame columns do not match expected depcode table."

    # Check that the truncate command was called if is_to_truncate is True
    if is_to_truncate:
        mock_conn.command.assert_called_once_with(f"TRUNCATE TABLE {table_name}")
        mock_conn.insert_df.assert_called_once_with(table=table_name, df=df)
    else:
        mock_conn.command.assert_not_called()
        mock_conn.insert_df.assert_called_once_with(table=table_name, df=df)


# test merge_daily_data method
@pytest.mark.parametrize("table_name, target_table_name",[
                             ("mart_newdata", "archived_data"),
                          ]
)
@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_merge_daily_data(mock_clickhouse_client, table_name, target_table_name):
    """
    Test merge_daily_data method.
    """
    mock_client = MagicMock()
    mock_conn = MagicMock()
    # mock config
    mock_client.get_conn.return_value = mock_conn
    mock_clickhouse_client.return_value = mock_client
    mock_conn.query_df.return_value = pd.DataFrame([{"column1": "value1"}]) # Simulate a non-empty DataFrame for the query

    clickhouse_queries = ClickHouseQueries()
    clickhouse_queries.merge_daily_data(table_name=table_name, target_table_name=target_table_name)
    mock_conn.query_df.assert_called_once_with(query=f"SELECT * FROM {table_name}")
    # Check that the correct query was executed
    query = f"""
        INSERT INTO {target_table_name}
        SELECT * FROM {table_name}
    """
    expected_query = textwrap.dedent(query).strip()
    mock_conn.command.assert_called_once_with(expected_query)

@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_check_table_has_empty_data(mock_clickhouse_client):
    """Test check_table_has_empty_data method. raises ValueError if the table is empty."""
    # mock config
    mock_client = MagicMock()
    mock_conn = MagicMock()
    mock_client.get_conn.return_value = mock_conn
    mock_clickhouse_client.return_value = mock_client
    mock_conn.query_df.return_value = pd.DataFrame(columns=["column1"])  # 0 rows

    # instantiate ClickHouseQueries
    clickhouse_queries = ClickHouseQueries()

    with pytest.raises(ValueError, match="Data from Table .* to be appended is empty."):
        clickhouse_queries.merge_daily_data(table_name="mart_newdata", target_table_name="archived_data")
        mock_conn.query_df.assert_called_once_with("SELECT * FROM mart_newdata")


@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_bootstrap_init_reference_tables_raises_when_reference_table_is_empty(mock_clickhouse_client):
    """Bootstrap should stop if a required reference table is still empty."""
    mock_client = MagicMock()
    mock_conn = MagicMock()
    mock_client.get_conn.return_value = mock_conn
    mock_clickhouse_client.return_value = mock_client

    mock_conn.query_df.side_effect = [
        pd.DataFrame([[0]], columns=["result"]),
        pd.DataFrame([[0]], columns=["cnt"]),
        pd.DataFrame([[0]], columns=["result"]),
        pd.DataFrame([[0]], columns=["cnt"]),
    ]

    clickhouse_queries = ClickHouseQueries()

    with pytest.raises(ValueError, match="Bootstrap failed: reference table raw_depcode is empty"):
        clickhouse_queries.bootstrap_init_reference_tables()


@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_reference_tables_guard_accepts_non_empty_reference_tables(mock_clickhouse_client):
    """Daily dbt runs should only require a guard check, not a fresh bootstrap."""
    mock_client = MagicMock()
    mock_conn = MagicMock()
    mock_client.get_conn.return_value = mock_conn
    mock_clickhouse_client.return_value = mock_client

    mock_client.run_query.side_effect = [
        pd.DataFrame([[1]], columns=["result"]),
        pd.DataFrame([[1]], columns=["result"]),
    ]
    mock_conn.query_df.side_effect = [
        pd.DataFrame([[10]], columns=["cnt"]),
        pd.DataFrame([[12]], columns=["cnt"]),
    ]

    clickhouse_queries = ClickHouseQueries()
    clickhouse_queries.ensure_reference_tables_ready()

    assert mock_conn.query_df.call_count == 2


@patch("project_functions.python.clickhouse_crud.get_clickhouse_client")
def test_reference_tables_guard_raises_if_reference_table_is_empty(mock_clickhouse_client):
    """The daily guard must stop dbt if a required dimension table is empty."""
    mock_client = MagicMock()
    mock_conn = MagicMock()
    mock_client.get_conn.return_value = mock_conn
    mock_clickhouse_client.return_value = mock_client

    mock_conn.query_df.side_effect = [
        pd.DataFrame([[0]], columns=["result"]),
        pd.DataFrame([[0]], columns=["cnt"]),
    ]

    clickhouse_queries = ClickHouseQueries()

    with pytest.raises(ValueError, match="Reference table raw_depcode is missing data"):
        clickhouse_queries.ensure_reference_tables_ready()
