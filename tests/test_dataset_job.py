import unittest
from io import BytesIO
from unittest.mock import MagicMock
from unittest.mock import patch
import pytest
import requests
from md_dataset.dataset_job import JobParams
from md_dataset.dataset_job import create_or_update_dataset_job
from md_dataset.dataset_job import create_or_update_dataset_job_send_http_request
from md_dataset.dataset_job import dataset_job_params
from md_dataset.dataset_job import name_to_slug
from md_dataset.models.dataset import DatasetType
from src.md_dataset.dataset_job import create_or_update_dataset_job_and_deployment
from src.md_dataset.dataset_job import create_or_update_dataset_job_and_deployment_send_http_request


class DatasetJobParamsTest(unittest.TestCase):
    def test_returns_parameters_schema(self):
        params, description, parameters_new = dataset_job_params(
            name="test_func",
            module="tests.func",
        )

        assert params["title"] == "Parameters"
        assert params["required"] == ["input_datasets", "params", "output_dataset_type"]
        assert params["properties"]["input_datasets"]["title"] == "input_datasets"
        assert params["properties"]["input_datasets"]["type"] == "array"
        assert params["properties"]["input_datasets"]["items"] == {"$ref": "#/definitions/InputDataset"}

        assert parameters_new["input_datasets"]["name"]["title"] == "Name"
        assert parameters_new["input_datasets"]["name"]["type"] == "string"

        assert description=="A nice description."


class CreateOrUpdateDatasetJobTest(unittest.TestCase):
    @patch("requests.post")
    def test_success(self, mock_post: MagicMock):
        response = requests.Response()
        response.status_code = 200
        response.raw = BytesIO(b'{"id": 123}')
        mock_post.return_value = response

        result = create_or_update_dataset_job(
            base_url="http://example.com:8001",
            job_params=JobParams(function="test_func", module="tests.func", name="job name", published=True),
            deployment_name="deployment name",
            run_type=DatasetType.INTENSITY,
        )

        assert result == {"id": 123}

        expected_payload = {
            "name": "job name",
            "slug": "job_name",
            "description": "A nice description.",
            "flow_and_deployment_name": "test_func/deployment name",
            "run_type": DatasetType.INTENSITY,
            "published": True,
        }

        (url,), kwargs = mock_post.call_args
        assert url == "http://example.com:8001/jobs/create_or_update"
        actual_payload = kwargs["json"]
        actual_params = actual_payload["params"]
        assert actual_params["title"] == "Parameters"
        actual_payload.pop("params")
        actual_payload.pop("params_new")
        assert actual_payload == expected_payload

    @patch("requests.post")
    def test_success_with_create_or_update_dataset_job_and_deployment(self, mock_post: MagicMock):
        response = requests.Response()
        response.status_code = 200
        response.raw = BytesIO(b'{"id": 123}')
        mock_post.return_value = response

        result = create_or_update_dataset_job_and_deployment(
            base_url="http://example.com:8001",
            job_params=JobParams(function="test_func", module="tests.func", name="job name", published=True),
            run_type=DatasetType.INTENSITY,
            image="111113333333.dkr.ecr.us-east-1.amazonaws.com/dose-response:0.0.2-2",
        )

        assert result == {"id": 123}

        expected_payload = {
            "name": "job name",
            "description": "A nice description.",
            "run_type": DatasetType.INTENSITY,
            "job_deploy_request": {
                "image": "111113333333.dkr.ecr.us-east-1.amazonaws.com/dose-response:0.0.2-2",
                "flow_package": "tests.func",
                "flow": "test_func",
            },
        }

        (url,), kwargs = mock_post.call_args
        assert url == "http://example.com:8001/jobs/v2/create_or_update"
        actual_payload = kwargs["json"]
        assert actual_payload == expected_payload

class FlowNameToSlugTest(unittest.TestCase):
    def test_name_to_slug(self):
        assert name_to_slug("Hello World!") == "hello_world"
        assert name_to_slug("  Spaces  ") == "spaces"
        assert name_to_slug("Mixed_CASE-123") == "mixed_case_123"
        assert name_to_slug("!!!Special###Chars%%%") == "special_chars"
        assert name_to_slug("multiple   spaces") == "multiple_spaces"
        assert name_to_slug("-starting-with-dash") == "starting_with_dash"
        assert name_to_slug("ending-with-dash-") == "ending_with_dash"
        assert name_to_slug("") == ""

class CreateOrUpdateDatasetJobSendHttpRequestTest(unittest.TestCase):
    @patch("requests.post")
    def test_successful_request(self, mock_post: MagicMock):
        response = requests.Response()
        response.status_code = 200
        response.raw = BytesIO(b'{"id": 123}')
        mock_post.return_value = response

        result = create_or_update_dataset_job_send_http_request(
            base_url="http://example.com",
            job_name="job name",
            description="description",
            flow_and_deployment_name="flow and deployment name",
            run_type=DatasetType.INTENSITY,
            params={"param1": "value1"},
            params_new={"param2": "value2"},
            published=False,
        )

        assert result == {"id": 123}

        expected_payload = {
            "name": "job name",
            "slug": "job_name",
            "description": "description",
            "flow_and_deployment_name": "flow and deployment name",
            "run_type": DatasetType.INTENSITY,
            "params": {"param1": "value1"},
            "params_new": {"param2": "value2"},
            "published": False,
        }

        mock_post.assert_called_once_with(
            "http://example.com/jobs/create_or_update", json=expected_payload, timeout=10,
        )

    @patch("requests.post")
    def test_failed_request(self, mock_post: MagicMock):
        response = requests.Response()
        response.status_code = 400
        response.raw = BytesIO(b"Bad Request")
        mock_post.return_value = response

        with pytest.raises(requests.exceptions.HTTPError):
            create_or_update_dataset_job_send_http_request(
                base_url="http://example.com",
                job_name="job name",
                description="description",
                flow_and_deployment_name="flow and deployment name",
                run_type="INTENSITY",
                params={"param1": "value1"},
                params_new={"param2": "value2"},
                published=False,
            )

class CreateOrUpdateDatasetJobAndDeploymentSendHttpRequestTest(unittest.TestCase):
    @patch("requests.get")
    @patch("requests.post")
    def test_accepted_request(self, mock_post: MagicMock, mock_get: MagicMock):
        response = requests.Response()
        response.status_code = 202
        response.headers = {"Location": "/jobs/some_slug/job_deploy_request/thePodName"}
        response.raw = BytesIO(b'{"id": 123}')
        mock_post.return_value = response

        response = requests.Response()
        response.status_code = 202
        response.headers = {"Location": "/jobs/some_slug/job_deploy_request/thePodName"}
        response.raw = BytesIO(b'{"id": 123}')

        finished_response = requests.Response()
        finished_response.status_code = 200
        finished_response.raw = BytesIO(b'{"id": 123}')
        mock_get.side_effect = [response, finished_response]

        result = create_or_update_dataset_job_and_deployment_send_http_request(
                base_url="http://example.com",
                job_name="job name",
                description="description",
                run_type=DatasetType.INTENSITY,
                job_deploy_request={
                    "image": "something",
                },

            )

        assert result == {"id": 123}

        expected_payload = {
            "name": "job name",
            "description": "description",
            "run_type": DatasetType.INTENSITY,
            "job_deploy_request": {"image": "something"},
        }

        mock_post.assert_called_once_with(
            "http://example.com/jobs/v2/create_or_update", json=expected_payload, timeout=50,
        )


        expected_get_call_count = 2
        assert mock_get.call_count == expected_get_call_count

