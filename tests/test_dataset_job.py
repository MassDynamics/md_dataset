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


class DatasetJobParamsTest(unittest.TestCase):
    def test_returns_parameters_schema(self):
        params, description = dataset_job_params(
            name="test_func",
            module="tests.func",
        )

        assert params["title"] == "Parameters"
        assert params["required"] == ["input_datasets", "params", "output_dataset_type"]
        assert params["properties"]["input_datasets"]["title"] == "input_datasets"
        assert params["properties"]["input_datasets"]["type"] == "array"
        assert params["properties"]["input_datasets"]["items"] == {"$ref": "#/definitions/InputDataset"}

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
            job_params=JobParams(function="test_func", module="tests.func", name="job name"),
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
        }

        (url,), kwargs = mock_post.call_args
        assert url == "http://example.com:8001/jobs/create_or_update"
        actual_payload = kwargs["json"]
        actual_params = actual_payload["params"]
        assert actual_params["title"] == "Parameters"
        actual_payload.pop("params")
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
        )

        assert result == {"id": 123}

        expected_payload = {
            "name": "job name",
            "slug": "job_name",
            "description": "description",
            "flow_and_deployment_name": "flow and deployment name",
            "run_type": DatasetType.INTENSITY,
            "params": {"param1": "value1"},
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
            )

