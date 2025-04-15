import re
from typing import NamedTuple
import requests
from prefect.utilities.callables import parameter_schema
from md_dataset.models.dataset import DatasetType


# ruff: noqa: PLR0913
def create_or_update_dataset_job_send_http_request(
    base_url: str,
    job_name: str,
    description: str,
    flow_and_deployment_name: str,
    run_type: DatasetType,
    params: dict,
) -> dict:
    """Send HTTP POST request to create or update a dataset job.

    Args:
        base_url: The endpoint base URL (schema, host, and port), e.g. http://example.com:8001
        job_name: Name of the job
        description: Description of the job
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
        "description": description,
        "flow_and_deployment_name": flow_and_deployment_name,
        "run_type": run_type,
        "params": params,
    }

    url = f"{base_url}/jobs/create_or_update"
    response = requests.post(url, json=payload, timeout=10)
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        error_body = response.text
        error_details = f". Response body: {error_body}" if error_body else ""
        msg = f"HTTP error occurred: {e}{error_details}"
        raise requests.exceptions.HTTPError(
            msg,
        ) from e


def dataset_job_params(name: str, module: str) -> tuple[dict, str]:
    """Get the parameters schema for a flow.

    Args:
        name: Name of the function, must be an existing function name in the `module`.
        module: The path to the Python modules containing the function (e.g. 'module.thing')
    """
    module = __import__(module, fromlist=[module])

    fn = getattr(module, name)
    description = fn.__doc__
    parameters = parameter_schema(fn)
    return parameters.dict(), description


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

class JobParams(NamedTuple):
    name: str
    function: str
    module: str

def create_or_update_dataset_job(
    base_url: str,
    job_params: JobParams,
    deployment_name: str,
    run_type: DatasetType,
) -> dict:
    """Send HTTP request to dataset service to create or update a dataset job.

    Args:
        base_url: The endpoint base URL of the dataset service API (schema, host, and port), e.g. http://example.com:8001
        deployment_name: Name of the deployment
        job_params: The job name, name of the module and function, must be ab existing function name in the `module`.
        run_type: Dataset type ('DatasetType.INTENSITY', 'DatasetType.PAIRWISE' etc.)

    Returns:
        dict: The JSON response from the server containing the dataset job.
    """
    params, description = dataset_job_params(name=job_params.function, module=job_params.module)
    flow_and_deployment_name = f"{job_params.function}/{deployment_name}"

    return create_or_update_dataset_job_send_http_request(
        base_url=base_url,
        job_name=job_params.name,
        description=description,
        flow_and_deployment_name=flow_and_deployment_name,
        run_type=run_type,
        params=params,
    )

