from io import BytesIO
import botocore
import pandas as pd
import pytest
from md_dataset.file_manager import FileManager


@pytest.fixture
def s3_client_mock(mocker):
    return mocker.Mock()

@pytest.fixture
def file_manager(s3_client_mock):
    return FileManager(s3_client_mock, default_bucket="default-bucket")

@pytest.fixture
def file_manager_no_default(s3_client_mock):
    return FileManager(s3_client_mock)

def test_load_parquet_to_df_with_explicit_bucket(mocker, s3_client_mock, file_manager):
    test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    parquet_buffer = BytesIO()
    test_df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)

    def mock_download_fileobj(Bucket, Key, Fileobj) -> None:
        Fileobj.write(parquet_buffer.getvalue())

    s3_client_mock.download_fileobj.side_effect = mock_download_fileobj

    result_df = file_manager.load_parquet_to_df(bucket="explicit-bucket", key="test-key")
    pd.testing.assert_frame_equal(result_df, test_df)
    s3_client_mock.download_fileobj.assert_called_once_with("explicit-bucket", "test-key", mocker.ANY)

def test_load_parquet_to_df_with_default_bucket(mocker, s3_client_mock, file_manager):
    test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    parquet_buffer = BytesIO()
    test_df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)

    def mock_download_fileobj(Bucket, Key, Fileobj) -> None:
        Fileobj.write(parquet_buffer.getvalue())

    s3_client_mock.download_fileobj.side_effect = mock_download_fileobj

    result_df = file_manager.load_parquet_to_df(bucket=None, key="test-key")
    pd.testing.assert_frame_equal(result_df, test_df)
    s3_client_mock.download_fileobj.assert_called_once_with("default-bucket", "test-key", mocker.ANY)

def test_missing_bucket_raises_exception(mocker, file_manager_no_default):
    with pytest.raises(Exception, match="Source bucket not provided"):
        with file_manager_no_default.load_parquet_to_df(bucket=None, key="test-key"):
            pass

def test_download_404_error_returns_false(mocker, s3_client_mock, file_manager):
    error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
    s3_client_mock.download_fileobj.side_effect = botocore.exceptions.ClientError(
        error_response, "Download",
    )

    result = file_manager._file_download(bucket="test-bucket", key="nonexistent-key").__enter__()
    assert result is False
    s3_client_mock.download_fileobj.assert_called_once_with("test-bucket", "nonexistent-key", mocker.ANY)

def test_download_other_client_error_raises(mocker, s3_client_mock, file_manager):
    error_response = {"Error": {"Code": "500", "Message": "Internal Server Error"}}
    s3_client_mock.download_fileobj.side_effect = botocore.exceptions.ClientError(
        error_response, "Download",
    )

    with pytest.raises(botocore.exceptions.ClientError, match="Internal Server Error"):
        with file_manager._file_download(bucket="test-bucket", key="error-key") as _:
            pass
