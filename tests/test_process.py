from uuid import UUID
import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness
from pytest_mock import MockerFixture
from md_dataset.file_manager import FileManager
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import InputDatasetTable
from md_dataset.models.dataset import InputParams
from md_dataset.models.dataset import IntensityInputDataset
from md_dataset.models.dataset import IntensityTableType
from md_dataset.process import md_py


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
    return [IntensityInputDataset(id=UUID("11111111-1111-1111-1111-111111111111"), name="one", tables=[
            InputDatasetTable(name="Protein_Intensity", bucket = "bucket", key = "baz/qux"),
            InputDatasetTable(name="Protein_Metadata", bucket= "bucket", key = "qux/quux"),
        ])]


class TestBlahParams(InputParams):
    id: int
    names: list[str] = None

@pytest.fixture
def test_params() -> TestBlahParams:
    return TestBlahParams(dataset_name="foo", id=123)

@md_py
def run_process_sets_name_and_type(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY)
    metadata_table = input_datasets[0].table(IntensityTableType.METADATA)
    return {IntensityTableType.INTENSITY.value: intensity_table.data, \
            IntensityTableType.METADATA.value: metadata_table.data}

def test_run_process_sets_name_and_type(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    result = run_process_sets_name_and_type(input_datasets, test_params, DatasetType.INTENSITY)
    assert result["name"] == "foo"
    assert result["type"] == DatasetType.INTENSITY

def test_run_process_has_tables(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    result = run_process_sets_name_and_type(input_datasets, test_params, DatasetType.INTENSITY)
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][1]["name"] == "Protein_Metadata"

@md_py
def run_process_data(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY)
    metadata_table = input_datasets[0].table(IntensityTableType.METADATA)

    return {IntensityTableType.INTENSITY.value: intensity_table.data.iloc[::-1], \
            IntensityTableType.METADATA.value: metadata_table.data}

def test_run_process_save_and_returns_data(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    result = run_process_data(input_datasets, test_params, DatasetType.INTENSITY)

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
    pd.testing.assert_frame_equal(args[0][0][1], test_data.iloc[::-1])

    assert args[0][1][0] == f"job_runs/{result['run_id']}/metadata.parquet"
    pd.testing.assert_frame_equal(args[0][1][1], test_metadata)

def test_run_process_support_md_names(input_datasets: list[IntensityInputDataset], \
        fake_file_manager: FileManager):
    test_params = TestBlahParams(id=123, names=["one", "two"])
    test_data = pd.DataFrame({})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    result = run_process_sets_name_and_type(input_datasets, test_params, DatasetType.INTENSITY)
    assert result["name"] == "one"

@md_py
def run_process_missing_metadata(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY)
    return {IntensityTableType.INTENSITY.value: intensity_table.data}

def test_run_process_invalid_missing_metadata(input_datasets: list[IntensityInputDataset], \
        test_params: TestBlahParams, fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with pytest.raises(Exception) as exception_info:
        assert run_process_missing_metadata(input_datasets, test_params, DatasetType.INTENSITY).is_failed()

    assert "1 validation error for IntensityDataset" in str(exception_info.value)
    assert "The field 'metadata' must be set and cannot be None." in str(exception_info.value)

@md_py
def run_process_invalid_dataframe(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY)
    return {IntensityTableType.INTENSITY.value: intensity_table.data, \
            IntensityTableType.METADATA.value: {"a": "dict"}}

def test_run_process_invalid_type(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with pytest.raises(TypeError) as exception_info:
        assert run_process_invalid_dataframe(input_datasets, test_params, DatasetType.INTENSITY).is_failed()

    assert "The field 'metadata' must be a pandas DataFrame, but got dict" in \
            str(exception_info.value)

def test_run_process_returns_table_data(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    result = run_process_data(input_datasets, test_params, DatasetType.INTENSITY)

    assert UUID(result["tables"][0]["id"], version=4) is not None
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][0]["path"] == f"job_runs/{result['run_id']}/intensity.parquet"

    assert UUID(result["tables"][1]["id"], version=4) is not None
    assert result["tables"][1]["name"] == "Protein_Metadata"
    assert result["tables"][1]["path"] == f"job_runs/{result['run_id']}/metadata.parquet"

@md_py
def run_process_data_with_runtime_metadata(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY)
    metadata_table = input_datasets[0].table(IntensityTableType.METADATA)

    return {IntensityTableType.INTENSITY.value: intensity_table.data, \
            IntensityTableType.METADATA.value: metadata_table.data, \
            IntensityTableType.RUNTIME_METADATA.value: pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})}

def test_run_process_returns_table_runtime_metadata(input_datasets: list[IntensityInputDataset], \
        test_params: TestBlahParams, fake_file_manager: FileManager):

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    result = run_process_data_with_runtime_metadata(input_datasets, test_params, DatasetType.INTENSITY)

    assert UUID(result["tables"][2]["id"], version=4) is not None
    assert result["tables"][2]["name"] == "Protein_RuntimeMetadata"
    assert result["tables"][2]["path"] == f"job_runs/{result['run_id']}/runtime_metadata.parquet"

    fake_file_manager.save_tables.assert_called_once()
    args, _ = fake_file_manager.save_tables.call_args

    assert isinstance(args[0], list)
    assert len(args[0]) == 3 # noqa: PLR2004

    assert args[0][2][0] == f"job_runs/{result['run_id']}/runtime_metadata.parquet"
    pd.testing.assert_frame_equal(args[0][1][1], pd.DataFrame({"col1": [4, 5, 6], \
            "col2": ["x", "y", "z"]}))
