import logging
import time
import requests
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
def create_or_update_dataset_job_and_deployment_send_http_request(
        base_url: str,
        api_key: str,
        job_name: str,
        run_type: DatasetType,
        slug: str|None = None,
        job_deploy_request: dict|None = None,
) -> dict:
    """Send HTTP POST request to create or update a dataset job.

    Args:
        base_url: The endpoint base URL (schema, host, and port), e.g. http://example.com:8001
        api_key: Your API key from the mass dynamics API.
        job_name: Name of the job
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
        "run_type": run_type,
        "job_deploy_request": job_deploy_request,
    }
    if slug:
        payload["slug"] = slug

    url = f"{base_url}/api/jobs/create_or_update"
    response = requests.post(url, json=payload, timeout=50, headers={"Authorization": f"Bearer {api_key}"})
    try:
        response.raise_for_status()
        log_status_code =f"url:{url} status_code: {response.status_code}"
        logger.info(log_status_code)
        if response.status_code == ACCEPTED_STATUS_CODE:
            return get_job_deploy_request(base_url, api_key, response.headers["Location"]).json()
        return response.json()
    except requests.exceptions.HTTPError as e:
        error_body = response.text
        error_details = f". Response body: {error_body}" if error_body else ""
        msg = f"HTTP error occurred: {e}{error_details}"
        raise requests.exceptions.HTTPError(
            msg,
        ) from e


def get_job_deploy_request(base_url: str, api_key: str, url: str) -> requests.Response:
    response = requests.get(f"{base_url}{url}", timeout=50, headers={"Authorization": f"Bearer {api_key}"})
    response.raise_for_status()
    log_status_code =f"url:{url} status_code: {response.status_code}"
    logger.info(log_status_code)
    if response.status_code == ACCEPTED_STATUS_CODE:
        logger.info("waiting for deployment...")
        time.sleep(2)
        return get_job_deploy_request(base_url, api_key,response.headers["Location"])
    return response


def create_or_update_dataset_job_and_deployment(
        base_url: str,
        api_key: str,
        job_name: str,
        job_function: str,
        job_module: str,
        run_type: DatasetType,
        image: str,
        dataset_slug: str | None = None,
) -> dict:
    """Send HTTP request to dataset service to create or update a dataset job end to end.

    Args:
        base_url: The endpoint base URL of the mass dynamics API (schema, host, and port), e.g. http://example.com:8001
        api_key: Your API key from the mass dynamics API.
        job_name: The job name
        job_function: The name of the function to run in the flow.
        job_module: The path to the Python modules containing the function (e.g. 'module.thing')
        run_type: Dataset type ('DatasetType.INTENSITY', 'DatasetType.PAIRWISE' etc.)
        image: full image tag which has already been made available (e.g. "1234.ecr.us-east-1.com/dose-response:0.0.2")
        dataset_slug: A unique string based on the existing job's name. Useful for updating the name of the job

    Returns:
        dict: The JSON response from the server containing the dataset job.
    """
    return create_or_update_dataset_job_and_deployment_send_http_request(
        base_url=base_url,
        api_key=api_key,
        job_name=job_name,
        run_type=run_type,
        slug=dataset_slug,
        job_deploy_request={
            "image": image,
            "flow_package": job_module,
            "flow": job_function,
        },
    )

