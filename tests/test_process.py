import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness
from pytest_mock import MockerFixture
from md_dataset.file_manager import FileManager
from md_dataset.models.types import BiomolecularSource
from md_dataset.models.types import DatasetType
from md_dataset.models.types import InputDatasetTable
from md_dataset.models.types import InputParams
from md_dataset.models.types import IntensityInputDataset
from md_dataset.models.types import IntensityTableType
from md_dataset.models.types import OutputDataset
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
    return [IntensityInputDataset(name="one", tables=[
            InputDatasetTable(name="Protein_Intensity", bucket = "bucket", key = "baz/qux"),
            InputDatasetTable(name="Protein_Metadata", bucket= "bucket", key = "qux/quux"),
        ])]


class TestBlahParams(InputParams):
    id: int
    source: BiomolecularSource

@pytest.fixture
def input_params() -> TestBlahParams:
    return TestBlahParams(dataset_name="foo", id=123, source=BiomolecularSource.PROTEIN)

def test_run_process_uses_config(input_datasets: list[IntensityInputDataset], input_params: TestBlahParams, \
        fake_file_manager: FileManager):
    @md_py
    def run_process_config( input_datasets: list[IntensityInputDataset], params: TestBlahParams, \
        output_dataset_type: DatasetType) -> OutputDataset: # noqa: ARG001
        output = OutputDataset.create(dataset_type=output_dataset_type, source=input_params.source)
        output.add(IntensityTableType.INTENSITY,  pd.concat([pd.DataFrame({"col1": [params.dataset_name]}), \
-                input_datasets[0].table_data_by_name("Protein_Intensity")]))
        return output

    test_data = pd.DataFrame({})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    assert run_process_config(
            input_datasets,
            input_params,
            DatasetType.INTENSITY,
            ).data(0)["col1"].to_numpy()[0] == "foo"

@md_py
def run_process_sets_name_and_type(input_datasets: list[IntensityInputDataset], input_params: InputParams, \
        output_dataset_type: DatasetType) -> OutputDataset:

    input_data = input_datasets[0].table(input_params.source, IntensityTableType.INTENSITY)

    output = OutputDataset.create(dataset_type=output_dataset_type, source=input_params.source)
    output.add(IntensityTableType.INTENSITY, [1, input_data])

    return output

def test_run_process_sets_name_and_type(input_datasets: list[IntensityInputDataset], input_params: TestBlahParams, \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    results = run_process_sets_name_and_type(input_datasets, input_params, DatasetType.INTENSITY)
    assert results.datasets[0].name == "foo"
    assert results.datasets[0].type == DatasetType.INTENSITY

def test_run_process_sets_table_name(input_datasets: list[IntensityInputDataset], input_params: TestBlahParams, \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    results = run_process_sets_name_and_type(input_datasets, input_params, DatasetType.INTENSITY)
    assert results.datasets[0].tables[0].name == "Protein_Intensity"

def test_run_process_sets_default_name(input_datasets: list[IntensityInputDataset], \
        fake_file_manager: FileManager):
    input_datasets[0]
    input_params = TestBlahParams(id=123, source=BiomolecularSource.PEPTIDE)
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    results = run_process_sets_name_and_type(input_datasets, input_params, DatasetType.INTENSITY)
    assert results.datasets[0].name == "one"

def test_run_process_correct_table(input_datasets: list[IntensityInputDataset], \
        fake_file_manager: FileManager):
    input_datasets[0]
    input_params = TestBlahParams(id=123, source=BiomolecularSource.PEPTIDE)
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    results = run_process_sets_name_and_type(input_datasets, input_params, DatasetType.INTENSITY)
    assert results.datasets[0].tables[0].name == "Peptide_Intensity"

@md_py
def run_process_sets_flow_output(input_datasets: list[IntensityInputDataset], input_params: InputParams, \
        output_dataset_type: DatasetType) -> OutputDataset:

    input_data = input_datasets[0].table(input_params.source, IntensityTableType.INTENSITY)
    input_metadata = input_datasets[0].table(input_params.source, IntensityTableType.METADATA)

    output = OutputDataset.create(dataset_type=output_dataset_type, source=input_params.source)
    output.add(IntensityTableType.INTENSITY, input_data.data.iloc[::-1])
    output.add(IntensityTableType.METADATA, input_metadata.data)

    return output

def test_run_process_sets_flow_output(input_datasets: list[IntensityInputDataset], input_params: TestBlahParams, \
        fake_file_manager: FileManager):

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    results = run_process_sets_flow_output(input_datasets, input_params, DatasetType.INTENSITY)
    pd.testing.assert_frame_equal(results.data(0), test_data.iloc[::-1])
    pd.testing.assert_frame_equal(results.data(1), test_metadata)
