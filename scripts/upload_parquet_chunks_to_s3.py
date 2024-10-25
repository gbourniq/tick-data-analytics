from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import NoCredentialsError

# Constants
LOCAL_DIRECTORY = Path(__file__).parent / "data" / "converted"
S3_URI = "s3://dev.data-staging.eu-west-1/timeseries/internal_id=ES_INDEX_FUTURES"


def upload_to_s3(local_file, bucket, s3_file):
    """
    Upload a local file to an S3 bucket.

    Args:
        local_file (str or Path): Path to the local file to be uploaded.
        bucket (str): Name of the S3 bucket.
        s3_file (str): S3 key (path) where the file will be stored.

    Returns:
        bool: True if upload was successful, False otherwise.
    """
    s3 = boto3.client("s3")
    try:
        s3.upload_file(str(local_file), bucket, s3_file)
        print(f"Upload Successful: {local_file} -> s3://{bucket}/{s3_file}")
        return True
    except FileNotFoundError:
        print(f"The file {local_file} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def parse_s3_uri(uri):
    """
    Parse an S3 URI into bucket name and prefix.

    Args:
        uri (str): The S3 URI to parse.

    Returns:
        tuple: A tuple containing the bucket name and prefix.
    """
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def main():
    """
    Main function to upload Parquet files to S3.

    Uploads all files matching the pattern '*filter0*.parquet' from the
    LOCAL_DIRECTORY to the S3 bucket specified in S3_URI.
    """
    bucket, prefix = parse_s3_uri(S3_URI)

    for file_path in LOCAL_DIRECTORY.glob("*filter0*.parquet"):
        s3_path = f"{prefix}{file_path.name}"
        upload_to_s3(file_path, bucket, s3_path)


if __name__ == "__main__":
    main()
