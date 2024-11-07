import os
from functools import wraps
from typing import Callable
from typing import ParamSpec
import boto3
import boto3.session
from prefect import flow
from prefect_aws.s3 import S3Bucket
from md_dataset.file_manager import FileManager
from md_dataset.models.types import DatasetParams
from md_dataset.models.types import FlowOutPut
from md_dataset.models.types import FlowOutPutDataSet
from md_dataset.models.types import FlowOutPutTable

profile = os.getenv("AWS_PROFILE")

P = ParamSpec("P")


def get_s3_block() -> S3Bucket:
    results_bucket = os.getenv("RESULTS_BUCKET")
    if not results_bucket:
        msg = "RESULTS_BUCKET environment variable not set"
        raise ValueError(msg)
    s3_block = S3Bucket(bucket_name=results_bucket, bucket_folder="prefect_result_storage")
    s3_block.save("md_process")
    return s3_block

def get_aws_session() -> boto3.session.Session:
    if os.getenv("EKS"):
        return boto3.session.Session()
    return boto3.session.Session(profile_name=profile)

def get_file_manager() -> None:
    client = get_aws_session().client("s3")
    return FileManager(client)

def md_process(func: Callable) -> Callable:
    result_storage = get_s3_block() if os.getenv("RESULTS_BUCKET") is not None else None

    @flow(
            log_prints=True,
            persist_result=True,
            result_storage=result_storage,
    )
    @wraps(func)
    def wrapper(params: DatasetParams, *args: P.args, **kwargs: P.kwargs) -> FlowOutPut:
        source_bucket = os.getenv("SOURCE_BUCKET")
        file_manager = get_file_manager()
        source_data = file_manager.load_parquet_to_df(bucket = source_bucket, key = params.tables["Protein_Intensity"])

        results = func(source_data, *args, **kwargs)

        metadata = file_manager.load_parquet_to_df(bucket = source_bucket, key = params.tables["Protein_Intensity"])

        return FlowOutPut(
            data_sets=[
                FlowOutPutDataSet(
                    name=params.name,
                    type=params.type,
                    tables=[
                        FlowOutPutTable(name="Protein_Intensity", data=results),
                        FlowOutPutTable(name="Protein_Metadata", data=metadata),
                    ],
                ),

            ],
        )

    return wrapper
