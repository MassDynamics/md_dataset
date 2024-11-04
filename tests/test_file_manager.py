from io import BytesIO
import pandas as pd
import pytest
from boto3_type_annotations.s3 import Client
from pytest_mock import MockerFixture
from md_dataset.file_manager import FileManager


@pytest.fixture
def s3_client(mocker: MockerFixture):
    return mocker.Mock()

@pytest.fixture
def file_manager(s3_client: Client):
    return FileManager(s3_client)

def test_load_parquet_to_df(mocker: MockerFixture, s3_client: Client, file_manager: FileManager):
    test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    parquet_buffer = BytesIO()
    test_df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)

    def mock_download_fileobj(_bucket: str, _key: str, fileobj: BytesIO) -> None:
        fileobj.write(parquet_buffer.getvalue())

    s3_client.download_fileobj.side_effect = mock_download_fileobj

    result_df = file_manager.load_parquet_to_df(bucket="test-bucket", key="test-key")

    pd.testing.assert_frame_equal(result_df, test_df)
    s3_client.download_fileobj.assert_called_once_with("test-bucket", "test-key", mocker.ANY)
