import pandas as pd
import pytest
import prefect
import uuid
from prefect.testing.utilities import prefect_test_harness
from pytest_mock import MockerFixture
from rpy2.robjects import conversion
from rpy2.robjects import default_converter
from md_dataset.file_manager import FileManager
from md_dataset.models.types import DatasetType
from md_dataset.models.types import InputDataset
from md_dataset.models.types import InputDatasetTable
from md_dataset.models.types import InputParams
from md_dataset.models.types import RPreparation
from md_dataset.process import md_r


class TestRParams(InputParams):
    message: str


@md_r(r_file="./tests/test_process.r", r_function="process")
def prepare_test_run_r(input_datasets: list[InputDataset], params: TestRParams, \
        output_dataset_type: DatasetType) -> RPreparation: # noqa: ARG001:
    return RPreparation(data_frames = [ \
            input_datasets[0].table_data_by_name("Protein_Intensity"), \
            input_datasets[0].table_data_by_name("Protein_Metadata")], \
            r_args=[params.message])

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
def input_datasets() -> list[InputDataset]:
    return [InputDataset(name="r", tables=[
            InputDatasetTable(name="Protein_Intensity", bucket = "bucket", key = "baz/qux"),
            InputDatasetTable(name="Protein_Metadata", bucket= "bucket", key = "qux/quux"),
        ], type=DatasetType.INTENSITY)]

def test_run_process_r_input_dataset_default_name(input_datasets: list[InputDataset], fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with conversion.localconverter(default_converter):
        result = prepare_test_run_r(input_datasets, TestRParams(message="hello"), DatasetType.INTENSITY)

    assert result['name'] == "r"
    assert result['type'] == DatasetType.INTENSITY
    assert result['run_id'] != None
    assert type(result['run_id']) == uuid.UUID

def test_run_process_r_input_dataset_provided_dataset_name(input_datasets: list[InputDataset], \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with conversion.localconverter(default_converter):
        result = prepare_test_run_r(input_datasets, TestRParams(dataset_name="test some r code", \
                message="hello"), DatasetType.INTENSITY)

    assert result['name'] == "test some r code"

def test_run_process_r_results(input_datasets: list[InputDataset], fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": ["x", "y", "z"], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": [1, 2, 3]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    with conversion.localconverter(default_converter):
        result= prepare_test_run_r(input_datasets, TestRParams(message="hello"), DatasetType.INTENSITY)

    assert result['tables'][0]['name'] == "Protein_Intensity"
    assert result['tables'][0]['path'] == f"job_runs/{result['run_id']}/intensity.parquet"

    assert result['tables'][1]['name'] == "Protein_Metadata"
    assert result['tables'][1]['path'] == f"job_runs/{result['run_id']}/metadata.parquet"

    fake_file_manager.save_tables.assert_called_once()
    args, _ = fake_file_manager.save_tables.call_args

    assert isinstance(args[0], list)
    assert len(args[0]) == 2

    pd.testing.assert_frame_equal(args[0][0].reset_index(drop=True), test_data[test_data.columns[::-1]])
    pd.testing.assert_frame_equal(args[0][1].reset_index(drop=True), pd.DataFrame({"Test": ["First"], "Message": ["hello"]}))
