from typing import TypeVar
import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness
from pytest_mock import MockerFixture
from md_dataset.file_manager import FileManager
from md_dataset.models.types import DatasetParams
from md_dataset.models.types import DatasetType
from md_dataset.process import md_process

pd.core.frame.PandasDataFrame = TypeVar("pd.core.frame.DataFrame")



@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@pytest.fixture
def fake_file_manager(mocker: MockerFixture):
    file_manager = mocker.Mock(spec=FileManager)

    mocker.patch("md_dataset.process.get_file_manager", return_value=file_manager)
    return file_manager


@pytest.fixture
def params() -> DatasetParams:
    return DatasetParams(name="one", tables={item[0]:item[1] for item in [
            ["Protein_Intensity", "baz/qux"],
            ["Protein_Metadata", "qux/quux"],
        ]}, type=DatasetType.INTENSITY)


def test_run_process_uses_source_data(params: DatasetParams, fake_file_manager: FileManager):
    @md_process
    def run_process(dataframe: pd.core.frame.PandasDataFrame) -> pd.core.frame.PandasDataFrame:
        return dataframe.iloc[::-1]

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    pd.testing.assert_frame_equal(run_process(params).data(0), test_data.iloc[::-1])


def test_run_process_sets_name(params: DatasetParams, fake_file_manager: FileManager):
    @md_process
    def run_process(dataframe: pd.core.frame.PandasDataFrame) -> list:
        return [1, dataframe]

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    assert run_process(params).data_sets[0].name == "one"


def test_run_process_sets_metadata(params: DatasetParams, fake_file_manager: FileManager):
    @md_process
    def run_process(dataframe: pd.core.frame.PandasDataFrame) -> list:
        return dataframe.iloc[::-1]

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    pd.testing.assert_frame_equal(run_process(params).data(1), test_metadata)
