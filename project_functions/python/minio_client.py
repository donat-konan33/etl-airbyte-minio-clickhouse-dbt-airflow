import os
from minio import Minio

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_API_PORT = os.environ.get("MINIO_API_PORT") # internal port for MinIO API
MINIO_HOST_IP = os.environ.get("MINIO_HOST_IP")
# Configuration for connecting to MinIO

def get_minio_client() -> Minio:
    """
    Returns a Minio client configured with the environment variables.
    """
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, MINIO_HOST, MINIO_API_PORT]):
        raise ValueError("One or more required environment variables are not set.")
    config = {
        "minio_endpoint": f"{MINIO_HOST}:{MINIO_API_PORT}", # set MINIO_HOST/MINIO_HOST_IP
        "minio_username": AWS_ACCESS_KEY_ID,
        "minio_password": AWS_SECRET_ACCESS_KEY,
    }
    client = Minio(
        endpoint=config["minio_endpoint"],
        access_key=config["minio_username"],
        secret_key=config["minio_password"],
        secure=False
    )
    return client

if __name__ == "__main__":
    minio_client = get_minio_client()
    print("MinIO client created successfully:", minio_client)
