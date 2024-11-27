import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness
from pytest_mock import MockerFixture
from md_dataset.file_manager import FileManager
from md_dataset.models.types import DatasetType
from md_dataset.models.types import InputDataset
from md_dataset.models.types import InputDatasetTable
from md_dataset.models.types import InputParams
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
def input_data_sets() -> list[InputDataset]:
    return [InputDataset(name="one", tables=[
            InputDatasetTable(name="Protein_Intensity", bucket = "bucket", key = "baz/qux"),
            InputDatasetTable(name="Protein_Metadata", bucket= "bucket", key = "qux/quux"),
        ], type=DatasetType.INTENSITY)]


class TestBlahParams(InputParams):
    id: int

@pytest.fixture
def input_params() -> TestBlahParams:
   return TestBlahParams(id=123, name="foo")

def test_run_process_uses_config(input_data_sets: list[InputDataset], input_params: TestBlahParams, \
        fake_file_manager: FileManager):
    @md_py
    def run_process_config(
            input_data_sets: list[InputDataset],
            params: TestBlahParams,
        ) -> dict:
        return {"Protein_Intensity": pd.concat([pd.DataFrame({"col1": [params.name]}), \
                input_data_sets[0].table_data_by_name("Protein_Metadata")])}

    test_data = pd.DataFrame({})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    assert run_process_config(
            input_data_sets,
            input_params,
            ).data(0)["col1"].to_numpy()[0] == "foo"

def test_run_process_sets_name_and_type(input_data_sets: list[InputDataset], input_params: TestBlahParams, \
        fake_file_manager: FileManager):
    @md_py
    def run_process_sets_name_and_type(input_data_sets: list[InputDataset], input_params: InputParams) -> dict: # noqa: ARG001
        return {"Protein_Intensity": [1, input_data_sets[0].table_data_by_name("Protein_Intensity")]}

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    results = run_process_sets_name_and_type(input_data_sets, input_params)
    assert results.data_sets[0].name == "foo"
    assert results.data_sets[0].type == DatasetType.INTENSITY

def test_run_process_sets_default_name(input_data_sets: list[InputDataset], \
        fake_file_manager: FileManager):
    @md_py
    def run_process_sets_name_and_type(input_data_sets: list[InputDataset], input_params: InputParams) -> dict: # noqa: ARG001
        return {"Protein_Intensity": [1, input_data_sets[0].table_data_by_name("Protein_Intensity")]}

    input_params = TestBlahParams(id=123)
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    results = run_process_sets_name_and_type(input_data_sets, input_params)
    assert results.data_sets[0].name == "one"

def test_run_process_sets_flow_output(input_data_sets: list[InputDataset], input_params: TestBlahParams, \
        fake_file_manager: FileManager):
    @md_py
    def run_process_sets_flow_output(input_data_sets: list[InputDataset], input_params: InputParams) -> dict: # noqa: ARG001
        return {
                "Protein_Intensity": input_data_sets[0].table_by_name("Protein_Intensity").data.iloc[::-1],
                "Protein_Metadata": input_data_sets[0].table_by_name("Protein_Metadata").data,
                }

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    results = run_process_sets_flow_output(input_data_sets, input_params)
    pd.testing.assert_frame_equal(results.data(0), test_data.iloc[::-1])
    pd.testing.assert_frame_equal(results.data(1), test_metadata)
