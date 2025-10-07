from __future__ import annotations
import os
from functools import wraps
from typing import TYPE_CHECKING
from typing import ParamSpec
from typing import TypeVar
from prefect import flow
from prefect import get_run_logger
from prefect import runtime
from prefect import task
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import EntityInputParams
from md_dataset.models.dataset import InputDataset
from md_dataset.models.dataset import InputParams
from md_dataset.models.factory import create_dataset_from_run
from md_dataset.storage import FileManager
from md_dataset.storage import get_file_manager
from md_dataset.storage import get_s3_block

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID
    from md_dataset.models.r import RFuncArgs

P = ParamSpec("P")
T = TypeVar("T", bound="InputDataset")


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

# Python based datasets
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

        dataset = create_dataset_from_run(run_id=runtime.flow_run.id, \
                dataset_type=output_dataset_type, tables=results)

        file_manager.save_tables(dataset.tables())

        return dataset.dump()

    return wrapper

# New uploaded "experiments"
def md_upload(func: Callable) -> Callable:
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

        dataset = create_dataset_from_run(run_id=runtime.flow_run.id, \
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

def _convert_r_string_to_python(r_obj) -> str: # noqa: ANN001
    """Convert R string object to Python string."""
    import rpy2.robjects as ro

    if isinstance(r_obj, ro.vectors.StrVector):
        return r_obj[0]  # Extract first element
    return ro.conversion.get_conversion().rpy2py(r_obj)


def _extract_table_items(tables_data) -> list: # noqa: ANN001
    """Extract table items from tables data structure."""
    if isinstance(tables_data, dict):
        # Handle OrdDict - access values directly
        if hasattr(tables_data, "items"):
            # This is an OrdDict or similar R object
            return [recursive_conversion(value) for key, value in tables_data.items()]
        # Regular Python dict
        return list(tables_data.values())
    # Already a list
    return tables_data


def _convert_to_intensity_data_objects(converted_list) -> list: # noqa: ANN001
    """Convert list of dictionaries to IntensityData objects."""
    from md_dataset.models.dataset import IntensityData
    from md_dataset.models.dataset import IntensityEntity
    from md_dataset.models.dataset import IntensityTable
    from md_dataset.models.dataset import IntensityTableType
    logger = get_run_logger()

    logger.info("Converting to IntensityData objects")
    intensity_data_list = []

    for item in converted_list:
        # Convert entity
        entity_str = _convert_r_string_to_python(item["entity"])
        entity = IntensityEntity(entity_str)

        # Convert tables
        tables = []
        table_items = _extract_table_items(item["tables"])

        for table_item in table_items:
            table_type_str = _convert_r_string_to_python(table_item["type"])
            table_type = IntensityTableType(table_type_str)
            table_data = table_item["data"]  # Already converted to DataFrame
            tables.append(IntensityTable(type=table_type, data=table_data))

        intensity_data_list.append(IntensityData(entity=entity, tables=tables))

    return intensity_data_list


def _is_intensity_data_structure(converted_list) -> bool: # noqa: ANN001
    """Check if the converted list looks like IntensityData objects."""
    return (len(converted_list) > 0 and
            isinstance(converted_list[0], dict) and
            "entity" in converted_list[0] and
            "tables" in converted_list[0])


def recursive_conversion(r_object) -> dict | list: # noqa: ANN001
    import rpy2.robjects as ro
    logger = get_run_logger()

    if isinstance(r_object, ro.vectors.ListVector):
        # Check if it's a named list (has names, i.e LEGACY) or regular list
        if r_object.names:
            logger.info("Convert named ListVector to dict")
            return {key: recursive_conversion(value) for key, value in r_object.items()}

        logger.info("Convert unnamed ListVector to list")
        converted_list = [recursive_conversion(value) for value in r_object]

        # Check if this looks like a list of IntensityData objects
        if _is_intensity_data_structure(converted_list):
            return _convert_to_intensity_data_objects(converted_list)

        return converted_list

    # Handle R data frames
    if hasattr(r_object, "colnames") and hasattr(r_object, "nrow"):
        logger.info("Convert R data frame to pandas DataFrame")
        return ro.conversion.get_conversion().rpy2py(r_object)

    logger.info("Convert: %s", type(r_object))
    return ro.conversion.get_conversion().rpy2py(r_object)

# R based datasets
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

            dataset = create_dataset_from_run(run_id=runtime.flow_run.id, \
                    dataset_type=output_dataset_type, tables=results)

            file_manager.save_tables(dataset.tables())

            return dataset.dump()

        return wrapper
    return decorator
