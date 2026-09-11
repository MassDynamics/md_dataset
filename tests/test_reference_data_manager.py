from io import BytesIO
import pandas as pd
import pytest
from boto3_type_annotations.s3 import Client
from pytest_mock import MockerFixture
from md_dataset.storage import ReferenceDataManager
from md_dataset.storage import get_reference_data_manager


@pytest.fixture
def s3_client_mock(mocker: MockerFixture):
    return mocker.Mock()


@pytest.fixture
def reference_data_manager(mocker: MockerFixture, s3_client_mock: Client):
    mocker.patch("md_dataset.storage.reference_data_manager.get_s3_client", return_value=s3_client_mock)
    mocker.patch.dict("os.environ", {"REFERENCE_DATA_BUCKET_NAME": "reference-bucket"})
    return ReferenceDataManager()


def test_load_parquet_to_df(mocker: MockerFixture, s3_client_mock: Client, \
        reference_data_manager: ReferenceDataManager):
    test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    parquet_buffer = BytesIO()
    test_df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)

    def mock_download_fileobj(_bucket: str, _key: str, fileobj: BytesIO) -> None:
        fileobj.write(parquet_buffer.getvalue())

    s3_client_mock.download_fileobj.side_effect = mock_download_fileobj
    s3_client_mock.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "reference_data/upload/uniprot/"},
            {"Key": "reference_data/upload/uniprot/_SUCCESS"},
            {"Key": "reference_data/upload/uniprot/part-0.parquet"},
            {"Key": "reference_data/upload/uniprot/part-1.parquet"},
        ],
    }

    result_df = reference_data_manager.load_parquet_to_df("uniprot")
    pd.testing.assert_frame_equal(result_df, test_df)
    s3_client_mock.list_objects_v2.assert_called_once_with(
        Bucket="reference-bucket", Prefix="reference_data/upload/uniprot/",
    )
    s3_client_mock.download_fileobj.assert_called_once_with(
        "reference-bucket", "reference_data/upload/uniprot/part-0.parquet", mocker.ANY,
    )


def test_load_parquet_to_df_no_parquet_raises(reference_data_manager: ReferenceDataManager, \
        s3_client_mock: Client):
    s3_client_mock.list_objects_v2.return_value = {
        "Contents": [{"Key": "reference_data/upload/uniprot/_SUCCESS"}],
    }

    with pytest.raises(FileNotFoundError, match="No parquet file found"):
        reference_data_manager.load_parquet_to_df("uniprot")


def test_get_reference_data_manager(mocker: MockerFixture, s3_client_mock: Client):
    mocker.patch("md_dataset.storage.reference_data_manager.get_s3_client", return_value=s3_client_mock)
    assert isinstance(get_reference_data_manager(), ReferenceDataManager)
