
import os
import clickhouse_connect # for http connection to ClickHouse
from typing import Dict, Union
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
    def create_client(cls, conn:Dict[str, Union[str, int]]=conn_params):
        return clickhouse_connect.get_client(
            host= conn['hostip'] or conn['hostname'],
            port=conn['port'],
            username=conn['username'],
            password=conn['password'],
            database=conn['database']
        )

    def get_conn(self): # it gives us clickhouse client
        return clickhouse_connect.get_client(
            host= self.params['hostip'] or self.params['hostname'],
            port=self.params['port'],
            username=self.params['username'],
            password=self.params['password'],
            database=self.params['database']
        )

    def run_query(self, sql):
        client = self.get_conn()
        return client.query_df(sql)
