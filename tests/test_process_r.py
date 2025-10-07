from uuid import UUID
import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness
from pytest_mock import MockerFixture
from rpy2.robjects import conversion
from rpy2.robjects import default_converter
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import InputDatasetTable
from md_dataset.models.dataset import InputParams
from md_dataset.models.dataset import IntensityEntity
from md_dataset.models.dataset import IntensityInputDataset
from md_dataset.models.dataset import IntensityTableType
from md_dataset.models.r import RFuncArgs
from md_dataset.process import md_r
from md_dataset.storage import FileManager


class TestRParams(InputParams):
    message: str
    names: list[str] = None


@md_r(r_file="./tests/test_process.r", r_function="process_legacy")
def prepare_test_run_r_legacy(input_datasets: list[IntensityInputDataset], params: TestRParams, \
        output_dataset_type: DatasetType) -> RFuncArgs: # noqa: ARG001
    return RFuncArgs(data_frames = [ \
            input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN).data, \
            input_datasets[0].table(IntensityTableType.METADATA, IntensityEntity.PROTEIN).data], \
            r_args=[params.message])


@md_r(r_file="./tests/test_process.r", r_function="process")
def prepare_test_run_r(input_datasets: list[IntensityInputDataset], params: TestRParams, \
        output_dataset_type: DatasetType) -> RFuncArgs: # noqa: ARG001
    return RFuncArgs(data_frames = [ \
            input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN).data, \
            input_datasets[0].table(IntensityTableType.METADATA, IntensityEntity.PROTEIN).data], \
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
def input_datasets() -> list[IntensityInputDataset]:
    return [IntensityInputDataset(id=UUID("11111111-1111-1111-1111-111111111111"), name="r", tables=[
            InputDatasetTable(name="Protein_Intensity", bucket = "bucket", key = "baz/qux"),
            InputDatasetTable(name="Protein_Metadata", bucket= "bucket", key = "qux/quux"),
        ])]


def test_run_process_r_legacy_input_dataset(input_datasets: list[IntensityInputDataset], \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with conversion.localconverter(default_converter):
        result = prepare_test_run_r_legacy(input_datasets, TestRParams(dataset_name="test some r code", \
                message="hello"), DatasetType.INTENSITY)

    assert result["type"] == DatasetType.INTENSITY
    assert result["run_id"] is not None
    assert isinstance(result["run_id"], UUID)


def test_run_process_r_legacy_results(input_datasets: list[IntensityInputDataset], fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": ["x", "y", "z"], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": [1, 2, 3]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    with conversion.localconverter(default_converter):
        result = prepare_test_run_r_legacy(input_datasets, TestRParams(dataset_name="name", \
                message="hello"), DatasetType.INTENSITY)

    assert UUID(result["tables"][0]["id"], version=4) is not None
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][0]["path"] == f"job_runs/{result['run_id']}/intensity.parquet"

    assert UUID(result["tables"][1]["id"], version=4) is not None
    assert result["tables"][1]["name"] == "Protein_Metadata"
    assert result["tables"][1]["path"] == f"job_runs/{result['run_id']}/metadata.parquet"

    fake_file_manager.save_tables.assert_called_once()
    args, _ = fake_file_manager.save_tables.call_args

    assert isinstance(args[0], list)
    assert len(args[0]) == 2 # noqa: PLR2004

    assert args[0][0][0] == f"job_runs/{result['run_id']}/intensity.parquet"
    pd.testing.assert_frame_equal(args[0][0][1].reset_index(drop=True), \
            test_data[test_data.columns[::-1]])

    assert args[0][1][0] == f"job_runs/{result['run_id']}/metadata.parquet"
    pd.testing.assert_frame_equal(args[0][1][1].reset_index(drop=True), \
            pd.DataFrame({"Test": ["First"], "Message": ["hello"]}))


def test_run_process_r_input_dataset(input_datasets: list[IntensityInputDataset], \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with conversion.localconverter(default_converter):
        result = prepare_test_run_r(input_datasets, TestRParams(dataset_name="test some r code", \
                message="hello"), DatasetType.INTENSITY)

    assert result["type"] == DatasetType.INTENSITY
    assert result["run_id"] is not None
    assert isinstance(result["run_id"], UUID)


def test_run_process_r_results(input_datasets: list[IntensityInputDataset], fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": ["x", "y", "z"], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": [1, 2, 3]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    with conversion.localconverter(default_converter):
        result = prepare_test_run_r(input_datasets, TestRParams(dataset_name="name", \
                message="hello"), DatasetType.INTENSITY)

    assert UUID(result["tables"][0]["id"], version=4) is not None
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][0]["path"] == f"job_runs/{result['run_id']}/Protein_Intensity.parquet"

    assert UUID(result["tables"][1]["id"], version=4) is not None
    assert result["tables"][1]["name"] == "Protein_Metadata"
    assert result["tables"][1]["path"] == f"job_runs/{result['run_id']}/Protein_Metadata.parquet"

    fake_file_manager.save_tables.assert_called_once()
    args, _ = fake_file_manager.save_tables.call_args

    assert isinstance(args[0], list)
    assert len(args[0]) == 2 # noqa: PLR2004

    assert args[0][0][0] == f"job_runs/{result['run_id']}/Protein_Intensity.parquet"
    pd.testing.assert_frame_equal(args[0][0][1].reset_index(drop=True), \
            test_data[test_data.columns[::-1]])

    assert args[0][1][0] == f"job_runs/{result['run_id']}/Protein_Metadata.parquet"
    pd.testing.assert_frame_equal(args[0][1][1].reset_index(drop=True), \
            pd.DataFrame({"Test": ["First"], "Message": ["hello"]}))
