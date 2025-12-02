import logging
import re
import time
from typing import NamedTuple
import requests
from md_form import translate_payload
from prefect.utilities.callables import parameter_schema
from md_dataset.models.dataset import DatasetType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)

ACCEPTED_STATUS_CODE = 202

# ruff: noqa: PLR0913
def create_or_update_dataset_job_send_http_request(
    base_url: str,
    job_name: str,
    description: str,
    flow_and_deployment_name: str,
    run_type: DatasetType,
    params: dict,
    params_new: dict,
    published: bool = True,
) -> dict:
    """Send HTTP POST request to create or update a dataset job.

    Args:
        base_url: The endpoint base URL (schema, host, and port), e.g. http://example.com:8001
        job_name: Name of the job
        description: Description of the job
        flow_and_deployment_name: Name of the flow and deployment
        run_type: Dataset type ('DatasetType.INTENSITY', 'DatasetType.PAIRWISE' etc.)
        params: Dictionary of parameters for the job
        params_new: Translated parameters for the job
        published: Whether the job is published, default is True

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
        "params_new": params_new,
        "published": published,
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

# ruff: noqa: PLR0913
def create_or_update_dataset_job_and_deployment_send_http_request(
        base_url: str,
        job_name: str,
        description: str,
        run_type: DatasetType,
        slug: str|None = None,
        job_deploy_request: dict|None = None,
) -> dict:
    """Send HTTP POST request to create or update a dataset job.

    Args:
        base_url: The endpoint base URL (schema, host, and port), e.g. http://example.com:8001
        job_name: Name of the job
        description: Description of the job
        run_type: Dataset type ('DatasetType.INTENSITY', 'DatasetType.PAIRWISE' etc.)
        slug: A unique string based on the existing job's name. Useful for updating the name of the job
        job_deploy_request: dict of image, flow_package, flow

    Returns:
        dict: The JSON response from the server

    Raises:
        requests.exceptions.HTTPError: If the response status code is not 200
    """
    payload = {
        "name": job_name,
        "description": description,
        "run_type": run_type,
        "job_deploy_request": job_deploy_request,
    }
    if slug:
        payload["slug"] = slug

    url = f"{base_url}/jobs/v2/create_or_update"
    response = requests.post(url, json=payload, timeout=50)
    try:
        response.raise_for_status()
        log_status_code =f"url:{url} status_code: {response.status_code}"
        logger.info(log_status_code)
        if response.status_code == ACCEPTED_STATUS_CODE:
            return get_job_deploy_request(base_url, response.headers["Location"]).json()
        return response.json()
    except requests.exceptions.HTTPError as e:
        error_body = response.text
        error_details = f". Response body: {error_body}" if error_body else ""
        msg = f"HTTP error occurred: {e}{error_details}"
        raise requests.exceptions.HTTPError(
            msg,
        ) from e

def get_job_deploy_request(base_url: str, url: str) -> requests.Response:
    response = requests.get(f"{base_url}{url}", timeout=50)
    response.raise_for_status()
    log_status_code =f"url:{url} status_code: {response.status_code}"
    logger.info(log_status_code)
    if response.status_code == ACCEPTED_STATUS_CODE:
        logger.info("waiting for deployment...")
        time.sleep(2)
        return get_job_deploy_request(base_url, response.headers["Location"])
    return response

def dataset_job_params(name: str, module: str) -> tuple[dict, str, dict]:
    """Get the parameters schema for a flow.

    Args:
        name: Name of the function, must be an existing function name in the `module`.
        module: The path to the Python modules containing the function (e.g. 'module.thing')
    """
    module = __import__(module, fromlist=[module])

    # for old required_params
    fn = getattr(module, name)
    description = fn.__doc__
    parameters = parameter_schema(fn)

    # for new md_form properties
    fn_new = getattr(module, f"{name}_properties", None)
    parameters_new = translate_payload(dict(fn_new.parameters)) if fn_new else None

    return parameters.dict(), description, parameters_new


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
    published: bool

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
    params, description, params_new = dataset_job_params(name=job_params.function, module=job_params.module)
    flow_and_deployment_name = f"{job_params.function}/{deployment_name}"

    return create_or_update_dataset_job_send_http_request(
        base_url=base_url,
        job_name=job_params.name,
        description=description,
        flow_and_deployment_name=flow_and_deployment_name,
        run_type=run_type,
        params=params,
        params_new=params_new,
        published=job_params.published,
    )

def create_or_update_dataset_job_and_deployment(
        base_url: str,
        job_params: JobParams,
        run_type: DatasetType,
        image: str,
        dataset_slug: str | None = None,
) -> dict:
    """Send HTTP request to dataset service to create or update a dataset job end to end.

    Args:
        base_url: The endpoint base URL of the dataset service API (schema, host, and port), e.g. http://example.com:8001
        job_params: The job name, name of the module and function, must be ab existing function name in the `module`.
        run_type: Dataset type ('DatasetType.INTENSITY', 'DatasetType.PAIRWISE' etc.)
        image: full image tag which has already been made available (e.g. "1234.ecr.us-east-1.com/dose-response:0.0.2")
        dataset_slug: A unique string based on the existing job's name. Useful for updating the name of the job

    Returns:
        dict: The JSON response from the server containing the dataset job.
    """
    _,description, _ = dataset_job_params(name=job_params.function, module=job_params.module)

    return create_or_update_dataset_job_and_deployment_send_http_request(
        base_url=base_url,
        job_name=job_params.name,
        description=description,
        run_type=run_type,
        slug=dataset_slug,
        job_deploy_request={
            "image": image,
            "flow_package": job_params.module,
            "flow": job_params.function,
        },
    )

