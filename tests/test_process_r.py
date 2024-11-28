import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness
from pydantic import BaseModel
from pytest_mock import MockerFixture
from rpy2.robjects import conversion
from rpy2.robjects import default_converter
from md_dataset.file_manager import FileManager
from md_dataset.models.types import DatasetType
from md_dataset.models.types import InputDataset
from md_dataset.models.types import InputDatasetTable
from md_dataset.models.types import RPreparation
from md_dataset.models.types import InputParams
from md_dataset.process import md_r


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
def input_data_sets() -> list[InputDataset]:
    return [InputDataset(name="r", tables=[
            InputDatasetTable(name="Protein_Intensity", bucket = "bucket", key = "baz/qux"),
            InputDatasetTable(name="Protein_Metadata", bucket= "bucket", key = "qux/quux"),
        ], type=DatasetType.INTENSITY)]


class TestRParams(InputParams):
    message: str


@md_r(r_file="./tests/test_process.r", r_function="process")
def prepare_test_run_r(
        input_data_sets: list[InputDataset],
        params: TestRParams,
        output_dataset_type: DatasetType
        ) -> RPreparation:
    return RPreparation(data_frames = [ \
            input_data_sets[0].table_data_by_name("Protein_Intensity"), \
            input_data_sets[0].table_data_by_name("Protein_Metadata")], \
            r_args=[params.message])

def test_run_process_r_input_dataset_default_name(input_data_sets: list[InputDataset], fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with conversion.localconverter(default_converter):
        results = prepare_test_run_r(input_data_sets, TestRParams(message="hello"), DatasetType.INTENSITY)

    assert results.data_sets[0].name == "r"
    assert results.data_sets[0].type == DatasetType.INTENSITY

def test_run_process_r_input_dataset_provided_dataset_name(input_data_sets: list[InputDataset], fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with conversion.localconverter(default_converter):
        results = prepare_test_run_r(input_data_sets, TestRParams(dataset_name='test some r code', message="hello"), DatasetType.INTENSITY)

    assert results.data_sets[0].name == 'test some r code'
    assert results.data_sets[0].type == DatasetType.INTENSITY

def test_run_process_r_results(input_data_sets: list[InputDataset], fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": ["x", "y", "z"], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": [1, 2, 3]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    with conversion.localconverter(default_converter):
        results = prepare_test_run_r(input_data_sets, TestRParams(message="hello"), DatasetType.INTENSITY)

    pd.testing.assert_frame_equal(results.data(0).reset_index(drop=True), \
            test_data[test_data.columns[::-1]].reset_index(drop=True))

