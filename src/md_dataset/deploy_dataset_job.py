import logging
import os
from md_dataset.dataset_job_api import create_or_update_dataset_job_and_deployment
from md_dataset.models.dataset import DatasetType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


MASSDYNAMICS_API_BASE_URL = os.environ.get("MASSDYNAMICS_API_BASE_URL", "https://app.massdynamics.com")
MASSDYNAMICS_API_KEY = os.environ["MASSDYNAMICS_API_KEY"]
DOCKER_IMAGE = os.environ["DOCKER_IMAGE"]
JOB_NAME = os.environ["JOB_NAME"]
FLOW = os.environ["FLOW"]
FLOW_PACKAGE = os.environ["FLOW_PACKAGE"]
PUBLIC = os.environ.get("PUBLIC", None)
DATASET_RUN_TYPE = os.environ["DATASET_RUN_TYPE"]
DATASET_SLUG = os.environ.get("DATASET_SLUG", None)
def main() -> None:

    logger.info("DEPLOYING dataset job")
    job = create_or_update_dataset_job_and_deployment(
        base_url=MASSDYNAMICS_API_BASE_URL,
        api_key=MASSDYNAMICS_API_KEY,
        job_name=JOB_NAME,
        job_module=FLOW_PACKAGE,
        job_function=FLOW,
        run_type=DatasetType[DATASET_RUN_TYPE].value,
        image=DOCKER_IMAGE,
        public=PUBLIC,
        dataset_slug=DATASET_SLUG,
    )

    logger.info(job)
