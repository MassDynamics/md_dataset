import importlib
import logging
import os
import tempfile
from prefect.infrastructure import KubernetesImagePullPolicy
from md_dataset.dataset_job import JobParams
from md_dataset.dataset_job import create_or_update_dataset_job
from md_dataset.models.types import DatasetType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)

STAGE = os.environ.get("STAGE", "production")
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-2")

PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "http://prefect-server:4200/api")
os.environ["PREFECT_API_URL"] = PREFECT_API_URL

K8_NAMESPACE = os.environ.get("K8_NAMESPACE", "md")
POOL_NAME = os.environ.get("POOL_NAME", "kubernetes-workpool")
QUEUE_NAME = os.environ.get("QUEUE_NAME", "default")

HONEYBADGER_KEY = os.environ.get("HONEYBADGER_KEY", "")

MEMORY_REQUESTS = os.environ.get("PREFECT_DEPLOYMENT_MEMORY_REQUESTS", "2Gi")
CPU_REQUESTS = os.environ.get("PREFECT_DEPLOYMENT_CPU_REQUESTS", "1000m")
MEMORY_LIMITS = os.environ.get("PREFECT_DEPLOYMENT_MEMORY_LIMITS", "4Gi")
CPU_LIMITS = os.environ.get("PREFECT_DEPLOYMENT_CPU_LIMITS", "2000m")

DATASET_SERVICE_API_BASE_URL = os.environ.get("DATASET_SERVICE_API_BASE_URL", "http://md-data-set-web")

# REQUIRED
DOCKER_IMAGE = os.environ["DOCKER_IMAGE"]
K8_SERVICE_ACCOUNT_NAME = os.environ["K8_SERVICE_ACCOUNT_NAME"]
JOB_NAME = os.environ["JOB_NAME"]
FLOW = os.environ["FLOW"]
FLOW_PACKAGE = os.environ["FLOW_PACKAGE"]
DEPLOYMENT_NAME = os.environ["DEPLOYMENT_NAME"]
RESULTS_BUCKET = os.environ["PREFECT_RESULTS_BUCKET"]
INITIAL_DATA_BUCKET_NAME = os.environ["INITIAL_DATA_BUCKET_NAME"]
DATASET_RUN_TYPE = os.environ["DATASET_RUN_TYPE"]

def main() -> None:
    logger.warning("Prefect url: %s", PREFECT_API_URL)

    flow = getattr(importlib.import_module(FLOW_PACKAGE), FLOW)

    env_vars = {
        "STAGE": STAGE,
        "PREFECT_HOME": f"{tempfile.gettempdir()}/prefect/",
        "INITIAL_DATA_BUCKET_NAME": INITIAL_DATA_BUCKET_NAME,
        "RESULTS_BUCKET": RESULTS_BUCKET,  # prefect results
        "PREFECT_LOCAL_STORAGE_PATH": f"{tempfile.gettempdir()}/prefect/storage/",
        "HONEYBADGER_KEY": HONEYBADGER_KEY,
        "PREFECT_API_URL": PREFECT_API_URL,
        "AWS_REGION": AWS_REGION,
        "AWS_DEFAULT_REGION": AWS_REGION,  # boto3, https://docs.aws.amazon.com/sdkref/latest/guide/feature-region.html#feature-region-sdk-compat
    }

    logger.info("DEPLOYING prefect flow")

    flow.deploy(
        name=DEPLOYMENT_NAME,
        image=DOCKER_IMAGE,
        build=False,
        push=False,
        work_pool_name=POOL_NAME,
        work_queue_name=QUEUE_NAME,
        job_variables={
            "env": env_vars,
            "image": DOCKER_IMAGE,
            "image_pull_policy": KubernetesImagePullPolicy.ALWAYS,
            "namespace": K8_NAMESPACE,
            "finished_job_ttl": 10 * 60,
            "pod_watch_timeout_seconds": 15 * 60,
            "service_account_name": K8_SERVICE_ACCOUNT_NAME,
            "cpu_request": CPU_REQUESTS,
            "memory_request": MEMORY_REQUESTS,
            "cpu_limit": CPU_LIMITS,
            "memory_limit": MEMORY_LIMITS,
        },
        tags=[f"service={DEPLOYMENT_NAME}", f"job_name={JOB_NAME}", "type=custom"],
    )

    job = create_or_update_dataset_job(
        base_url=DATASET_SERVICE_API_BASE_URL,
        job_params=JobParams(function=FLOW, module=FLOW_PACKAGE, name=JOB_NAME),
        deployment_name=DEPLOYMENT_NAME,
        run_type=DatasetType[DATASET_RUN_TYPE].value,
    )

    logger.info("DEPLOYING dataset job")
    logger.info(job)
