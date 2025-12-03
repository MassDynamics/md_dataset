import unittest
from io import BytesIO
from unittest.mock import MagicMock
from unittest.mock import patch
import requests
from md_dataset.models.dataset import DatasetType
from src.md_dataset.dataset_job_api import create_or_update_dataset_job_and_deployment
from src.md_dataset.dataset_job_api import create_or_update_dataset_job_and_deployment_send_http_request


class CreateOrUpdateDatasetJobTest(unittest.TestCase):

    @patch("requests.post")
    def test_success_with_create_or_update_dataset_job_and_deployment(self, mock_post: MagicMock):
        response = requests.Response()
        response.status_code = 200
        response.raw = BytesIO(b'{"id": 123}')
        mock_post.return_value = response

        result = create_or_update_dataset_job_and_deployment(
            base_url="http://example.com:8001",
            api_key="api_key_AAAA",
            job_name="job name",
            job_function="test_func",
            job_module="tests.func",
            run_type=DatasetType.INTENSITY,
            image="111113333333.dkr.ecr.us-east-1.amazonaws.com/dose-response:0.0.2-2",
            dataset_slug="dataset_slug",
        )

        assert result == {"id": 123}

        expected_payload = {
            "name": "job name",
            "run_type": DatasetType.INTENSITY,
            "slug": "dataset_slug",
            "job_deploy_request": {
                "image": "111113333333.dkr.ecr.us-east-1.amazonaws.com/dose-response:0.0.2-2",
                "flow_package": "tests.func",
                "flow": "test_func",
            },
        }

        (url,), kwargs = mock_post.call_args
        assert url == "http://example.com:8001/api/jobs/create_or_update"
        actual_payload = kwargs["json"]
        assert actual_payload == expected_payload
        assert kwargs["headers"] == {"Authorization": "Bearer api_key_AAAA"}


class CreateOrUpdateDatasetJobAndDeploymentSendHttpRequestTest(unittest.TestCase):
    @patch("requests.get")
    @patch("requests.post")
    def test_accepted_request(self, mock_post: MagicMock, mock_get: MagicMock):
        response = requests.Response()
        response.status_code = 202
        response.headers = {"Location": "api/jobs/some_slug/job_deploy_request/thePodName"}
        response.raw = BytesIO(b'{"id": 123}')
        mock_post.return_value = response

        response = requests.Response()
        response.status_code = 202
        response.headers = {"Location": "api/jobs/some_slug/job_deploy_request/thePodName"}
        response.raw = BytesIO(b'{"id": 123}')

        finished_response = requests.Response()
        finished_response.status_code = 200
        finished_response.raw = BytesIO(b'{"id": 123}')
        mock_get.side_effect = [response, finished_response]

        result = create_or_update_dataset_job_and_deployment_send_http_request(
                base_url="http://example.com",
                api_key="api_key_AAAA",
                job_name="job name",
                run_type=DatasetType.INTENSITY,
                job_deploy_request={
                    "image": "something",
                },

            )

        assert result == {"id": 123}

        expected_payload = {
            "name": "job name",
            "run_type": DatasetType.INTENSITY,
            "job_deploy_request": {"image": "something"},
        }

        mock_post.assert_called_once_with(
            "http://example.com/api/jobs/create_or_update", json=expected_payload, timeout=50,
            headers={"Authorization": "Bearer api_key_AAAA"},
        )

        expected_get_call_count = 2
        assert mock_get.call_count == expected_get_call_count


        assert mock_get.call_args[1]["headers"]["Authorization"] == "Bearer api_key_AAAA"
