
import os
import clickhouse_connect # for http connection to ClickHouse
from typing import Dict, Union
import logging

# logger
logger = logging.getLogger(__name__)

conn_params = {
    'hostip': os.environ.get("CLICKHOUSE_HOST_IP"),
    'hostname': os.environ.get("CLICKHOUSE_HOST"),
    'port': 8123,  # Default ClickHouse HTTP port), for http connection
    'username': os.environ.get("CLICKHOUSE_USER"),
    'password': os.environ.get("CLICKHOUSE_PASSWORD"),
    'database': os.environ.get("CLICKHOUSE_DB", "default")
}
# Configuration for connecting to ClickHouse, useful for testing and production environments.
class ClickHouseClient:
    def __init__(self):
        self.params = conn_params

    @classmethod
    def create_client(cls, conn:Dict[str, Union[str, int]]=conn_params, host_stuff_name:str="hostname"):
        return clickhouse_connect.get_client(
            host=conn[host_stuff_name],
            port=conn['port'],
            username=conn['username'],
            password=conn['password'],
            database=conn['database']
        )

    def get_conn(self): # it gives us clickhouse client
        hosts = [
            "hostname",
            "hostip"
        ]

        for host_key in hosts:

            host = self.params[host_key]

            try:

                logger.info(
                    f"Trying ClickHouse connection: {host}"
                )

                client = clickhouse_connect.get_client(
                    host=host,
                    port=self.params['port'],
                    username=self.params['username'],
                    password=self.params['password'],
                    database=self.params['database']
                )

                # health check
                client.command("SELECT 1")

                logger.info(
                    "ClickHouse connection successful"
                )

                return client

            except Exception:

                logger.warning(
                    f"Failed to connect to {host}"
                )

        raise ConnectionError(
            "Unable to connect to ClickHouse using any host"
        )

    def run_query(self, sql):
        client = self.get_conn()
        return client.query_df(sql)
