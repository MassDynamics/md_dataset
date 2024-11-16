from io import BytesIO
import botocore
import pandas as pd
import pytest
from boto3_type_annotations.s3 import Client
from pytest_mock import MockerFixture
from md_dataset.file_manager import FileManager


@pytest.fixture
def s3_client_mock(mocker: MockerFixture):
    return mocker.Mock()

@pytest.fixture
def file_manager(s3_client_mock: Client):
    return FileManager(s3_client_mock, default_bucket="default-bucket")

@pytest.fixture
def file_manager_no_default(s3_client_mock: Client):
    return FileManager(s3_client_mock)

def test_load_parquet_to_df_with_explicit_bucket(mocker: MockerFixture, s3_client_mock: Client, \
        file_manager: FileManager):
    test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    parquet_buffer = BytesIO()
    test_df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)

    def mock_download_fileobj(_bucket: str, _key: str, fileobj: BytesIO) -> None:
        fileobj.write(parquet_buffer.getvalue())

    s3_client_mock.download_fileobj.side_effect = mock_download_fileobj

    result_df = file_manager.load_parquet_to_df(bucket="explicit-bucket", key="test-key")
    pd.testing.assert_frame_equal(result_df, test_df)
    s3_client_mock.download_fileobj.assert_called_once_with("explicit-bucket", "test-key", mocker.ANY)

def test_load_parquet_to_df_with_default_bucket(mocker: MockerFixture, s3_client_mock: Client, \
        file_manager: FileManager):
    test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    parquet_buffer = BytesIO()
    test_df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)

    def mock_download_fileobj(_bucket: str, _key: str, fileobj: BytesIO) -> None:
        fileobj.write(parquet_buffer.getvalue())

    s3_client_mock.download_fileobj.side_effect = mock_download_fileobj

    result_df = file_manager.load_parquet_to_df(bucket=None, key="test-key")
    pd.testing.assert_frame_equal(result_df, test_df)
    s3_client_mock.download_fileobj.assert_called_once_with("default-bucket", "test-key", mocker.ANY)

def test_missing_bucket_raises_exception(file_manager_no_default: FileManager):
    with pytest.raises(Exception, match="Source bucket not provided"), \
        file_manager_no_default.load_parquet_to_df(bucket=None, key="test-key"):
            pass

def test_download_other_client_error_raises(s3_client_mock: Client, file_manager: FileManager):
    error_response = {"Error": {"Code": "500", "Message": "Internal Server Error"}}
    s3_client_mock.download_fileobj.side_effect = botocore.exceptions.ClientError(
        error_response, "Download",
    )

    with pytest.raises(botocore.exceptions.ClientError, match="Internal Server Error"), \
        file_manager.load_parquet_to_df(bucket="test-bucket", key="error-key"):
            pass
