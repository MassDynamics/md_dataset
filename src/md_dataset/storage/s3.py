"""S3 storage utilities."""

import os
import boto3
import boto3.session
from prefect_aws.s3 import S3Bucket


def get_s3_block() -> S3Bucket:
    """Get S3 block for result storage."""
    results_bucket = os.getenv("RESULTS_BUCKET")
    if not results_bucket:
        msg = "RESULTS_BUCKET environment variable not set"
        raise ValueError(msg)

    # local dev using AWS
    profile = os.getenv("BOTO3_PROFILE")
    if profile is None:
        s3_block = S3Bucket(bucket_name=results_bucket, bucket_folder="prefect_result_storage")
        s3_block.save("mdprocess", overwrite=True)
        return s3_block
    return None


def get_s3_client() -> boto3.session.Session:
    """Get S3 client for file operations."""
    if os.environ.get("USE_LOCALSTACK", "false").lower() == "true":
        session = boto3.session.Session()
        return session.client(service_name="s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    # local dev using AWS
    profile = os.getenv("BOTO3_PROFILE")
    session = boto3.Session(profile_name=profile)
    return session.client("s3")
