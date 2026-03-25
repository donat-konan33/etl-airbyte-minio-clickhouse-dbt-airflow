# here connection to clickhouse via airflow with clickhouse_hook
from python.minio_client import get_minio_client
import pandas as pd
import numpy as np
from typing import List
import pendulum
import io

def choose_first(address):
    """
    choose the first element from string separated by comma
    """
    # head,*tail = Iter.split(",") # or head = Iter.split(",")[0] for iterable using
    # let's vectorize this calculation task
    return np.vectorize(lambda x: x.split(',')[0])(address)

# retrieve data
class MinioUtils:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.minio_client = get_minio_client()

    def get_object_list(self, prefix: str = "raw/weatherdata/") -> list:
        """
        Get a list of objects in the specified MinIO bucket with the given prefix.
        """
        if not self.bucket_name:
            raise ValueError("Bucket name must be provided.")
        try:
            objects = self.minio_client.list_objects(self.bucket_name, prefix=prefix)
            return [obj.object_name for obj in objects]
        except Exception as e:
            print(f"Error retrieving object list from MinIO: {e}")
            return []

    def retrieve_parquet_data(self, object_name: str) -> pd.DataFrame:
        """
        Retrieve data from MinIO bucket.
        """
        if not self.bucket_name or not object_name:
            raise ValueError("Bucket name and object name must be provided.")
        try:
            response = self.minio_client.get_object(self.bucket_name, object_name) # HTTPResponse
            return pd.read_parquet(io.BytesIO(response.read())) # Convert response to DataFrame

        except Exception as e:
            print(f"Error retrieving data from MinIO: {e}")
            return None

    def get_object_names(self, format: str = ".parquet", prefix: str = "raw/weatherdata/") -> list:
        """
        Get object names from MinIO bucket with the specified format and prefix.
        """
        all_objects = self.get_object_list(prefix=prefix)
        return [obj for obj in all_objects if obj.endswith(format)]


    def get_jsonl_data(self, object_name: str) -> pd.DataFrame:
        """
        Retrieve JSONL data from MinIO bucket and get the staging transformation to get dataframe
        """
        if not self.bucket_name or not object_name:
            raise ValueError("Bucket name and object name must be provided.")
        try:
            response = self.minio_client.get_object(self.bucket_name, object_name) # HTTPResponse
            return pd.read_json(io.BytesIO(response.read()), lines=True) # Convert response to DataFrame

        except Exception as e:
            print(f"Error retrieving data from MinIO: {e}")
            return None

    def get_intermediate_dataframe(self, staging_dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Get intermediate data
        """
        records = []
        for _, row in staging_dataframe.iterrows():
            data = {
                "_airbyte_ab_id": row["_airbyte_raw_id"],
                "latitude": row["_airbyte_data"]["latitude"],
                "longitude": row["_airbyte_data"]["longitude"],
                "resolvedAddress": row["_airbyte_data"]["resolvedAddress"],
                "days":row["_airbyte_data"]["days"]
            }
            records.append(data)
        data = pd.DataFrame(records).explode("days")
        records = []
        for _, row in data.iterrows():

            record = {
            "_airbyte_ab_id": row["_airbyte_ab_id"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "resolvedAddress": row["resolvedAddress"],
            **row["days"]
            }
            records.append(record)
        data = pd.DataFrame(records)
        return data


    def create_table_from_jsonl(self, object_name: str) -> pd.DataFrame:
        """
        transform .jsonl to DataFrame
        """
        df = self.get_jsonl_data(object_name)
        if df is not None:
            df = self.get_intermediate_dataframe(df)
            # final transformations
            df["department"] = df["resolvedAddress"].apply(choose_first)
            df = df[[
                '_airbyte_ab_id',
                'latitude',
                'longitude',
                'resolvedAddress',
                'cloudcover',
                'conditions',
                'datetime',
                'datetimeEpoch',
                'description',
                'dew',
                'feelslike',
                'feelslikemax',
                'feelslikemin',
                'humidity',
                'icon',
                'moonphase',
                'precip',
                'precipcover',
                'precipprob',
                'preciptype',
                'pressure',
                'severerisk',
                'snow',
                'snowdepth',
                'solarenergy',
                'solarradiation',
                'source',
                'stations',
                'sunrise',
                'sunriseEpoch',
                'sunset',
                'sunsetEpoch',
                'temp',
                'tempmax',
                'tempmin',
                'uvindex',
                'visibility',
                'winddir',
                'windgust',
                'windspeed',
                'department'
            ]]
        return df

    def upload_to_minio(self, buffer: io.BytesIO, data_size: int, file_path: str) -> None:
        """
        Upload a file to MinIO
        data_size : Size of the data to upload | mandatory
        """
        self.minio_client.put_object(
            self.bucket_name,
            object_name=file_path,
            data=buffer,
            length=data_size,
            content_type='application/octet-stream'
        )
        print(f"DataFrame successfully stored in MinIO at {file_path}")

    def storage_df_to_parquet(self, df: pd.DataFrame, prefix: List[str]=["raw/weatherdata/", "staging/daily/"]) -> None:
        """
        Store DataFrame as Parquet files in MinIO in raw then in staging
        """
        if df is None or df.empty:
            print("DataFrame is empty. Nothing to store.")
            return
        try:
            buffer = io.BytesIO()                 # serialize parquet in memory
            df.to_parquet(buffer, index=False)
            buffer.seek(0)   # always replace the cursor before uploading or reading
            data_size = buffer.getbuffer().nbytes # get size needed by minio
            for pref in prefix:
                # Define file path
                if pref == "staging/daily/":
                    buffer.seek(0) # replace the cursor before uploading
                    file_path = pref + "france_weather_daily_data_extracted.parquet"
                    self.upload_to_minio(buffer, data_size, file_path)
                else:
                    buffer.seek(0) # replace the cursor before uploading
                    date = pendulum.today().to_date_string()
                    file_path = pref + f"france_weather_{date}.parquet"
                    self.upload_to_minio(buffer, data_size, file_path)
        except Exception as e:
            print(f"Error storing DataFrame to MinIO: {e}")

    def delete_chunked_data(self, prefix: str="raw/weatherdata/", format: str=".jsonl") -> None:
        """ Delete jsonl file from bucket
        """
        try:
            objects = self.get_object_list(prefix=prefix)
            objects_to_delete = [obj for obj in objects if obj.endswith(format)]
            if objects_to_delete:
                for obj in objects_to_delete:
                    self.minio_client.remove_object(self.bucket_name, obj)
                    print(f"✅ Object {obj} deleted from bucket {self.bucket_name}.")
            else:
                print("✅ No object matching criteria specified above.")
        except Exception as e:
            print(f"Error deleting objects from MinIO: {e}")
