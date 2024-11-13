from __future__ import annotations
import os
from functools import wraps
from typing import Callable
from typing import ParamSpec
import boto3
import boto3.session
from prefect import flow
from prefect_aws.s3 import S3Bucket
from md_dataset.file_manager import FileManager
from md_dataset.models.types import FlowOutPut
from md_dataset.models.types import FlowOutPutDataSet
from md_dataset.models.types import FlowOutPutTable
from md_dataset.models.types import InputDataset

P = ParamSpec("P")


def get_s3_block() -> S3Bucket:
    results_bucket = os.getenv("RESULTS_BUCKET")
    if not results_bucket:
        msg = "RESULTS_BUCKET environment variable not set"
        raise ValueError(msg)
    s3_block = S3Bucket(bucket_name=results_bucket, bucket_folder="prefect_result_storage")
    s3_block.save("mdprocess", overwrite=True)
    return s3_block

def get_aws_session() -> boto3.session.Session:
    profile = os.getenv("AWS_PROFILE")
    if os.getenv("AWS_PROFILE"):
        return boto3.session.Session(profile_name=profile)
    return boto3.session.Session()

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
    def wrapper(input_data_sets: list[InputDataset], *args: P.args, **kwargs: P.kwargs) -> FlowOutPut:
        file_manager = get_file_manager()

        input_data_sets = [dataset.populate_tables(file_manager) for dataset in input_data_sets]
        # we will need to change this interface
        results = func(input_data_sets, *args, **kwargs)

        return FlowOutPut(
            # this may need to be constructed by the client
            data_sets=[
                FlowOutPutDataSet(
                    name=input_data_sets[0].name,
                    type=input_data_sets[0].type,
                    tables=[
                        FlowOutPutTable(name="Protein_Intensity", data=results),
                        FlowOutPutTable(name="Protein_Metadata", \
                                data=input_data_sets[0].table_by_name("Protein_Metadata").data),
                    ],
                ),

            ],
        )

    return wrapper
