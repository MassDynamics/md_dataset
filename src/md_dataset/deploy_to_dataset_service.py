import logging
import os
from md_dataset.dataset_job import JobParams
from md_dataset.dataset_job import create_or_update_dataset_job_and_deployment
from md_dataset.models.dataset import DatasetType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


DATASET_SERVICE_API_BASE_URL = os.environ.get("DATASET_SERVICE_API_BASE_URL", "http://md-data-set-web")
DOCKER_IMAGE = os.environ["DOCKER_IMAGE"]
JOB_NAME = os.environ["JOB_NAME"]
FLOW = os.environ["FLOW"]
FLOW_PACKAGE = os.environ["FLOW_PACKAGE"]
DEPLOYMENT_NAME = os.environ["DEPLOYMENT_NAME"]
PUBLISHED = os.environ.get("PUBLISHED", "false")
DATASET_RUN_TYPE = os.environ["DATASET_RUN_TYPE"]

def main() -> None:

    logger.info("DEPLOYING dataset job")
    job = create_or_update_dataset_job_and_deployment(
        base_url=DATASET_SERVICE_API_BASE_URL,
        job_params=JobParams(function=FLOW, module=FLOW_PACKAGE, name=JOB_NAME, published=PUBLISHED),
        deployment_name=DEPLOYMENT_NAME,
        run_type=DatasetType[DATASET_RUN_TYPE].value,
        image=DOCKER_IMAGE,
    )

    logger.info(job)
