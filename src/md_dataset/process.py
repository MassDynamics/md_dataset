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
from prefect import runtime
from prefect import task
from prefect_aws.s3 import S3Bucket
from md_dataset.file_manager import FileManager
from md_dataset.models.dataset import Dataset
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import EntityInputParams
from md_dataset.models.dataset import InputDataset
from md_dataset.models.dataset import InputParams

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID
    from md_dataset.models.r import RFuncArgs

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

def get_s3_client() -> boto3.session.Session:
    if os.environ.get("USE_LOCALSTACK", "false").lower() == "true":
        session = boto3.session.Session()
        return session.client(service_name="s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    return boto3.session.Session().client("s3")

def get_file_manager() -> None:
    return FileManager(client=get_s3_client(), default_bucket=os.getenv("RESULTS_BUCKET"))

def get_deployment_image() -> str:
    return os.getenv("IMAGE", "unknown")

def load_data(input_datasets: list[T], file_manager: FileManager) -> None:
    logger = get_run_logger()
    for dataset in input_datasets:
        try:
            dataset.populate_tables(file_manager)
        except Exception:
            logger.exception("Failed to load dataset %s", dataset.name)
            raise

def md_py(func: Callable) -> Callable:
    result_storage = get_s3_block() if os.getenv("RESULTS_BUCKET") is not None else None

    @flow(
            log_prints=True,
            persist_result=True,
            result_storage=result_storage,
    )
    @wraps(func)
    def wrapper(input_datasets: list[T], params: InputParams, output_dataset_type: DatasetType, \
            *args: P.args, **kwargs: P.kwargs) -> dict:
        logger = get_run_logger()
        logger.info("Running Deployment: %s", runtime.deployment.name)
        logger.info("Version: %s", runtime.deployment.version)
        logger.info("Image: %s", get_deployment_image())

        file_manager = get_file_manager()

        load_data(input_datasets, file_manager)

        results = func(input_datasets, params, output_dataset_type, *args, **kwargs)

        dataset = Dataset.from_run(run_id=runtime.flow_run.id, \
                dataset_type=output_dataset_type, tables=results)

        file_manager.save_tables(dataset.tables())

        return dataset.dump()

    return wrapper


def md_converter(func: Callable) -> Callable:
    result_storage = get_s3_block() if os.getenv("RESULTS_BUCKET") is not None else None

    @flow(
            log_prints=True,
            persist_result=True,
            result_storage=result_storage,
    )
    @wraps(func)
    def wrapper(experiment_id: UUID, params: EntityInputParams, \
            *args: P.args, **kwargs: P.kwargs) -> dict:
        logger = get_run_logger()
        logger.info("Running Deployment: %s", runtime.deployment.name)
        logger.info("Version: %s", runtime.deployment.version)
        logger.info("Image: %s", get_deployment_image())

        results = func(experiment_id, params, *args, **kwargs)

        dataset = Dataset.from_run(run_id=runtime.flow_run.id, \
                dataset_type=DatasetType.INTENSITY, tables=results)

        get_file_manager().save_tables(dataset.tables())

        return dataset.dump()

    return wrapper

@task
def run_r_task(
    r_file: str,
    r_function: str,
    r_preparation: RFuncArgs,
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

    r_out = r_func(*r_data_frames, *r_preparation.r_args)

    with (ro.default_converter + pandas2ri.converter).context():
        return recursive_conversion(r_out)

def recursive_conversion(r_object) -> dict: # noqa: ANN001
    import rpy2.robjects as ro
    logger = get_run_logger()

    if isinstance(r_object, ro.vectors.ListVector):
        logger.info("Convert ListVector")
        return {key: recursive_conversion(value) for key, value in r_object.items()}
    logger.info("Convert: %s", type(r_object))
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
                *args: P.args, **kwargs: P.kwargs) -> dict:
            logger = get_run_logger()
            logger.info("Running Deployment: %s", runtime.deployment.name)
            logger.info("Version: %s", runtime.deployment.version)
            logger.info("Image: %s", get_deployment_image())

            file_manager = get_file_manager()

            load_data(input_datasets, file_manager)

            r_args = func(input_datasets, params, output_dataset_type, *args, **kwargs)

            results = run_r_task(r_file, r_function, r_args)

            dataset = Dataset.from_run(run_id=runtime.flow_run.id, \
                    dataset_type=output_dataset_type, tables=results)

            file_manager.save_tables(dataset.tables())

            return dataset.dump()

        return wrapper
    return decorator
