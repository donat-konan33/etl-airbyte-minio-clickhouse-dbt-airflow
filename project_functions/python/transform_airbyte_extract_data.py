import pandas as pd
from typing import List
import asyncio
from python.minio_utils import MinioUtils
import re
from minio.deleteobjects import DeleteObject

# get files from minio with a specific format
def get_format_file(format: str, list_of_objects: List[str]) -> List[str]:
    """
    Get files from MinIO with a specific format.
    """
    return [obj for obj in list_of_objects if obj.endswith(format)]

# coroutines
async def async_read_files_from_minio(bucket_name: str, prefix: str="raw/weatherdata/", format: str=".parquet") -> pd.DataFrame:
    """
    Read files asynchronously from MinIO.
    minio_utils: Instance of MinioUtils class helps us to get data whatever any format from minio and provides with dataframe
    """
    minio_utils = MinioUtils(bucket_name=bucket_name)

    # Get the list of object names in the specified format
    object_names = minio_utils.get_object_names(format=format, prefix=prefix)

    # Create tasks to read the files
    if format == ".jsonl":
        tasks = [asyncio.to_thread(minio_utils.create_table_from_jsonl, object_name) for object_name in object_names]
        print("Coroutines created for JSONL files.")
    else:
        tasks = [asyncio.to_thread(minio_utils.retrieve_parquet_data, object_name) for object_name in object_names]

    # Gather the results
    results = await asyncio.gather(*tasks)
    print(f"Read {len(results)} files from MinIO.")

    # Concatenate the DataFrames
    final_df = pd.concat(results, ignore_index=True)
    return final_df

def get_and_store_data(bucket_name: str, prefix: str="raw/weatherdata/", format: str=".jsonl") -> None:
    """
    Get and store data from MinIO.
    """
    minio_client = MinioUtils(bucket_name=bucket_name)
    print("Connection to Minio established. Reading files from MinIO...")
    df = asyncio.run(async_read_files_from_minio(bucket_name=bucket_name, prefix=prefix, format=format))
    print(df.head()) # debug
    minio_client.storage_df_to_parquet(df)

def delete_files_from_minio(bucket_name: str, prefix: str, format: str) -> None:
    """
    Delete files from MinIO with a specific file extension.
    file_extension : equals to format of the file to delete
    """
    minio_client = MinioUtils(bucket_name=bucket_name)
    list_of_objects = [obj for obj in minio_client.get_object_list(prefix=prefix) if obj.endswith(format)]
    objects_to_delete = [DeleteObject(obj) for obj in list_of_objects]
    if objects_to_delete:
        results = minio_client.minio_client.remove_objects(bucket_name, objects_to_delete)
        for r in results:
            print(f"Deleted '{r.object_name}' from bucket '{bucket_name}'")
        print(f"Deleted {len(objects_to_delete)} '{format}' files from bucket '{bucket_name}' with prefix '{prefix}'.")
    else:
        print(f"No '{format}' files found in bucket '{bucket_name}' with prefix '{prefix}'.")

def get_name_of_blob(gs_path: List[str]) -> List[str]:
    """
    from gcs path find the name of file
    """
    new_list = [path.split("/")[-1] for path in gs_path]
    return new_list

def retrieve_diff_date(files_name: List[str]):
    """
    get different files according to the date of extraction
    """

    pattern = r'^\d{4}_\d{2}_\d{2}$' # give only this pattern `YYYY_MM_DD`
    date_list = [file_name[:10] for file_name in files_name if re.search(pattern, file_name[:10]) != None]
    different_date = set(date_list)
    return different_date

if __name__ == "__main__":
    bucket_name = "weather"
    prefix = "raw/weatherdata/"
    format_ = ".jsonl"
    # for test with classic python
    dataframe = asyncio.run(async_read_files_from_minio(bucket_name=bucket_name, prefix=prefix, format=format_))
    print(dataframe.head())
