from __future__ import annotations
import os
from functools import wraps
from typing import TYPE_CHECKING
from typing import ParamSpec
from typing import TypeVar
import boto3
import boto3.session
from prefect import flow
from prefect import get_run_logger
from prefect import task
from prefect_aws.s3 import S3Bucket
from md_dataset.file_manager import FileManager
from md_dataset.models.types import Dataset
from md_dataset.models.types import DatasetType
from md_dataset.models.types import FlowOutPut
from md_dataset.models.types import FlowOutPutDataSet
from md_dataset.models.types import FlowOutPutTable
from md_dataset.models.types import InputDataset
from md_dataset.models.types import InputParams
from md_dataset.models.types import RPreparation

if TYPE_CHECKING:
    from collections.abc import Callable

P = ParamSpec("P")
T = TypeVar("T", bound="InputDataset")

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

def md_py(func: Callable) -> Callable:
    result_storage = get_s3_block() if os.getenv("RESULTS_BUCKET") is not None else None

    @flow(
            log_prints=True,
            persist_result=True,
            result_storage=result_storage,
    )
    @wraps(func)
    def wrapper(input_datasets: list[T], params: InputParams, output_dataset_type: DatasetType, \
            *args: P.args, **kwargs: P.kwargs) -> FlowOutPut:
        file_manager = get_file_manager()

        for dataset in input_datasets:
            dataset.populate_tables(file_manager)

        results = func(input_datasets, params, output_dataset_type, *args, **kwargs)

        table_dict = results.dict()
        # validate tables based on output_dataset_type
        tables = [FlowOutPutTable(name=key, data=table_dict[key]) for key in table_dict]

        return FlowOutPut(
            datasets=[
                FlowOutPutDataSet(
                    name=params.dataset_name or input_datasets[0].name,
                    type=output_dataset_type,
                    tables=tables,
                ),

            ],
        )

    return wrapper



@task
def run_r_task(
    r_file: str,
    r_function: str,
    r_preparation: RPreparation,
) -> dict:
    import rpy2.robjects as ro
    from rpy2.robjects import pandas2ri
    from rpy2.robjects.conversion import localconverter

    logger = get_run_logger()
    logger.info("Running R task with function %s in file %s", r_function, r_file)

    r = ro.r
    r.source(r_file)
    r_func = getattr(r, r_function)

    with localconverter(ro.default_converter + pandas2ri.converter):
        r_data_frames = [ro.conversion.py2rpy(df) for df in r_preparation.data_frames]

    r_out_list = r_func(*r_data_frames, *r_preparation.r_args)

    value_type = [(key, type(value)) for key, value in r_out_list.items()]
    logger.info(value_type)
    logger.info("R items")
    value = list(r_out_list.items())
    logger.info(value)

    with (ro.default_converter + pandas2ri.converter).context():
        return {key: ro.conversion.get_conversion().rpy2py(value) for key, value in r_out_list.items()}

def recursive_conversion(r_object) -> dict: # noqa: ANN001
    import rpy2.robjects as ro
    logger = get_run_logger()

    if isinstance(r_object, ro.vectors.ListVector):
        logger.info("Type ListVector")
        return {key: recursive_conversion(value) for key, value in r_object.items()}
    if isinstance(r_object, ro.vectors.DataFrame):
        logger.info("Type DataFrame")
        return ro.conversion.get_conversion().rpy2py(r_object)
    if isinstance(r_object, ro.vectors.Vector):
        logger.info("Type Vector")
        return list(r_object)
    logger.info("Type Unkown")
    return ro.conversion.get_conversion().rpy2py(r_object)

def md_r(r_file: str, r_function: str) -> Callable:
    def decorator(func: Callable) -> Callable:
        result_storage = get_s3_block() if os.getenv("RESULTS_BUCKET") is not None else None

        @flow(
                log_prints=True,
                persist_result=True,
                result_storage=result_storage,
                )
        @wraps(func)
        def wrapper(input_datasets: list[T] , params: InputParams, output_dataset_type: DatasetType, \
                *args: P.args, **kwargs: P.kwargs) -> FlowOutPut:
            file_manager = get_file_manager()
            get_run_logger()
            for dataset in input_datasets:
                dataset.populate_tables(file_manager)

            r_preparation = func(input_datasets, params, output_dataset_type, *args, **kwargs)

            results = run_r_task(r_file, r_function, r_preparation)
            dataset = Dataset.build(name=params.dataset_name or input_datasets[0].name, \
                    dataset_type=output_dataset_type, tables=results)

            return FlowOutPut(
                datasets=[
                    dataset.output(),
                ],
            )

        return wrapper
    return decorator
