import re
import requests
from typing import NamedTuple
from prefect.utilities.callables import parameter_schema
from md_dataset.models.types import DatasetType


def create_or_update_dataset_job_send_http_request(
    base_url: str,
    job_name: str,
    flow_and_deployment_name: str,
    run_type: DatasetType,
    params: dict,
) -> dict:
    """Send HTTP POST request to create or update a dataset job.

    Args:
        base_url: The endpoint base URL (schema, host, and port), e.g. http://example.com:8001
        job_name: Name of the job
        flow_and_deployment_name: Name of the flow and deployment
        run_type: Dataset type ('DatasetType.INTENSITY', 'DatasetType.PAIRWISE' etc.)
        params: Dictionary of parameters for the job

    Returns:
        dict: The JSON response from the server

    Raises:
        requests.exceptions.HTTPError: If the response status code is not 200
    """
    payload = {
        "name": job_name,
        "slug": name_to_slug(job_name),
        "flow_and_deployment_name": flow_and_deployment_name,
        "run_type": run_type,
        "params": params,
    }

    url = f"{base_url}/jobs/create_or_update"
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()
    return response.json()


def dataset_job_params(flow_name: str, flow_module: str) -> dict:
    """Get the parameters schema for a flow.

    Args:
        flow_name: Name of the flow, must be an existing function name in the `flow_module`.
        flow_module: The path to the Python modules containing the function (e.g. 'flows.intensity_imputation')
    """
    flow_module = __import__(flow_module, fromlist=[flow_module])

    fn = getattr(flow_module, flow_name)
    parameters = parameter_schema(fn)
    return parameters.dict()


def name_to_slug(name: str) -> str:
    """Convert a name to a URL/filename-friendly slug.

    Args:
        name: The name to convert to a slug

    Returns:
        A lowercase string with non-alphanumeric characters replaced by underscores

    Example:
        >>> name_to_slug("Hello World!")
        "hello_world"
    """
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name.lower())
    return slug.strip("_")  # Remove leading/trailing underscores


def create_or_update_dataset_job(
    base_url: str,
    flow_name: str,
    flow_module: str,
    deployment_name: str,
    job_name: str,
    run_type: DatasetType,
) -> dict:
    """Send HTTP request to dataset service to create or update a dataset job.

    Args:
        base_url: The endpoint base URL of the dataset service API (schema, host, and port), e.g. http://example.com:8001
        deployment_name: Name of the deployment
        flow_name: Name of the flow, must be ab existing function name in the `flow_module`.
        flow_module: The path to the Python modules containing the function (e.g. 'flows.intensity_imputation')
        job_name: A unique dataset job name
        run_type: Dataset type ('DatasetType.INTENSITY', 'DatasetType.PAIRWISE' etc.)

    Returns:
        dict: The JSON response from the server containing the dataset job.
    """
    params = dataset_job_params(flow_name, flow_module)
    flow_and_deployment_name = f"{flow_name}/{deployment_name}"

    return create_or_update_dataset_job_send_http_request(
        base_url=base_url,
        job_name=job_name,
        flow_and_deployment_name=flow_and_deployment_name,
        run_type=run_type,
        params=params,
    )

