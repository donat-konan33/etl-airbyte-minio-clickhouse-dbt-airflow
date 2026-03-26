import os
from minio import Minio
from typing import Union
import logging

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_API_PORT = os.environ.get("MINIO_API_PORT") # internal port for MinIO API
MINIO_HOST_IP = os.environ.get("MINIO_HOST_IP")

# logger
logger = logging.getLogger(__name__)

# Configuration for connecting to MinIO
def get_minio_client() -> Minio:
    """
    Returns a Minio client configured with the environment variables.
    """
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, MINIO_HOST, MINIO_API_PORT]):
        raise ValueError("One or more required environment variables are not set.")


    # handle connection
    endpoints = [
            MINIO_HOST_IP,
            MINIO_HOST
        ]

    for host in endpoints:
        endpoint = f"{host}:{MINIO_API_PORT}"
        try:

            logger.info(
                f"Trying MinIO endpoint: {endpoint}"
            )

            client = Minio(
                endpoint=endpoint,
                access_key=AWS_ACCESS_KEY_ID,
                secret_key=AWS_SECRET_ACCESS_KEY,
                secure=False
            )

            # test real connection
            client.list_buckets()

            logger.info(
                "MinIO connection successful"
            )
            client = Minio(
                            endpoint=endpoint,
                            access_key=AWS_ACCESS_KEY_ID,
                            secret_key=AWS_SECRET_ACCESS_KEY,
                            secure=False
                        )

            # test real connection before return client
            print(f"Data Lake current Buckets are : {client.list_buckets()}")
            logger.info(
                "MinIO connection successful"
            )

            return client
        except Exception:

            logger.warning(
                f"Failed to connect to {endpoint}"
            )

    raise ConnectionError(
        "Unable to connect to MinIO using any endpoint"
    )


if __name__ == "__main__":
    minio_client = get_minio_client()
    print("MinIO client created successfully:", minio_client)
