import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness
from pydantic import BaseModel
from pytest_mock import MockerFixture
from md_dataset.file_manager import FileManager
from md_dataset.models.types import DatasetParams
from md_dataset.models.types import DatasetType
from md_dataset.process import md_process


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


class TestBlahParams(BaseModel):
    id: int
    name: str

def test_run_process_uses_source_data(params: DatasetParams, fake_file_manager: FileManager):
    @md_process
    def run_process(_dataframe: pd.core.frame.PandasDataFrame, blah: TestBlahParams) -> pd.core.frame.PandasDataFrame:
        return pd.DataFrame({"col1": [blah.name]})

    test_data = pd.DataFrame({})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    assert run_process(
            params, TestBlahParams(id=123, name="foo"),
            ).data(0)["col1"].to_numpy()[0] == "foo"


def test_run_process_sets_name_and_type(params: DatasetParams, fake_file_manager: FileManager):
    @md_process
    def run_process(dataframe: pd.core.frame.PandasDataFrame) -> list:
        return [1, dataframe]

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    results = run_process(params)
    assert results.data_sets[0].name == "one"
    assert results.data_sets[0].type == DatasetType.INTENSITY


def test_run_process_sets_flow_output(params: DatasetParams, fake_file_manager: FileManager):
    @md_process
    def run_process(dataframe: pd.core.frame.PandasDataFrame) -> pd.core.frame.PandasDataFrame:
        return dataframe

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    results = run_process(params)
    pd.testing.assert_frame_equal(results.data(0), test_data)
    pd.testing.assert_frame_equal(results.data(1), test_metadata)
