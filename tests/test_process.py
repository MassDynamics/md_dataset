from uuid import UUID
import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness
from pydantic import ValidationError
from pytest_mock import MockerFixture
from md_dataset.file_manager import FileManager
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import EntityInputParams
from md_dataset.models.dataset import InputDatasetTable
from md_dataset.models.dataset import InputParams
from md_dataset.models.dataset import IntensityData
from md_dataset.models.dataset import IntensityEntity
from md_dataset.models.dataset import IntensityInputDataset
from md_dataset.models.dataset import IntensityTable
from md_dataset.models.dataset import IntensityTableType
from md_dataset.process import md_converter
from md_dataset.process import md_py

# Test constants
THREE_EXPECTED_TABLES_COUNT = 3
FOUR_EXPECTED_TABLES_COUNT = 4


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
    dataset_name: str

@pytest.fixture
def test_params() -> TestBlahParams:
    return TestBlahParams(dataset_name="foo", id=123)


def test_run_process_legacy_invalid_missing_metadata(input_datasets: list[IntensityInputDataset], \
        test_params: TestBlahParams, fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with pytest.raises(Exception) as exception_info:
        assert run_process_legacy_missing_metadata(input_datasets, test_params, DatasetType.INTENSITY).is_failed()

    assert "1 validation error for LegacyIntensityDataset" in str(exception_info.value)
    assert "The field 'metadata' must be set and cannot be None." in str(exception_info.value)

@md_py
def run_process_data(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    metadata_table = input_datasets[0].table(IntensityTableType.METADATA, IntensityEntity.PROTEIN)

    return [
            IntensityData(
                entity=IntensityEntity.PROTEIN,
                tables = [
                    IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table.data.iloc[::-1]),
                    IntensityTable(type=IntensityTableType.METADATA, data=metadata_table.data),
                    ],
                ),
            ]

def test_run_process_save_and_returns_data(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    result = run_process_data(input_datasets, test_params, DatasetType.INTENSITY)

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
    pd.testing.assert_frame_equal(args[0][0][1], test_data.iloc[::-1])

    assert args[0][1][0] == f"job_runs/{result['run_id']}/Protein_Metadata.parquet"
    pd.testing.assert_frame_equal(args[0][1][1], test_metadata)

@md_py
def run_process_missing_metadata(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    return [
        IntensityData(
            entity=IntensityEntity.PROTEIN,
            tables = [
                IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table.data.iloc[::-1]),
                ],
            ),
        ]

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

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    return [
            IntensityData(
                entity=IntensityEntity.PROTEIN,
                tables = [
                    IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table.data.iloc[::-1]),
                    IntensityTable(type=IntensityTableType.METADATA, data={"a": "dict"}),
                    ],
                ),
            ]

def test_run_process_invalid_type(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with pytest.raises(ValidationError) as exception_info:
        assert run_process_invalid_dataframe(input_datasets, test_params, DatasetType.INTENSITY).is_failed()

    assert "Input should be an instance of DataFrame [type=is_instance_of, input_value={'a': 'dict'}, input_type=dict]"\
            in str(exception_info.value)

def test_run_process_returns_table_data(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):

    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    result = run_process_data(input_datasets, test_params, DatasetType.INTENSITY )

    assert UUID(result["tables"][0]["id"], version=4) is not None
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][0]["path"] == f"job_runs/{result['run_id']}/Protein_Intensity.parquet"

    assert UUID(result["tables"][1]["id"], version=4) is not None
    assert result["tables"][1]["name"] == "Protein_Metadata"
    assert result["tables"][1]["path"] == f"job_runs/{result['run_id']}/Protein_Metadata.parquet"

@md_py
def run_process_data_with_runtime_metadata(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    metadata_table = input_datasets[0].table(IntensityTableType.METADATA, IntensityEntity.PROTEIN)

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



@md_converter
def run_converter_process(experiment_id: UUID, params: InputParams) -> dict: # noqa: ARG001
    intensity_table = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    metadata_table = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    runtime_metadata_table = pd.DataFrame({"col1": [7], "col2": ["m"]})
    return [
            IntensityData(
                entity=IntensityEntity.PROTEIN,
                tables = [
                    IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table),
                    IntensityTable(type=IntensityTableType.METADATA, data=metadata_table),
                    IntensityTable(type=IntensityTableType.RUNTIME_METADATA, data=runtime_metadata_table),
                ],
            ),
            IntensityData(
                entity=IntensityEntity.PEPTIDE,
                tables = [
                    IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table),
                    IntensityTable(type=IntensityTableType.METADATA, data=metadata_table),
                    ],
                ),
    ]

def test_run_md_converter_process(fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    params = EntityInputParams(dataset_name="foo", entity_type="Peptide")

    result = run_converter_process(UUID("11111111-1111-1111-1111-111111111111"), params)

    assert UUID(result["tables"][0]["id"], version=4) is not None
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][0]["path"] == f"job_runs/{result['run_id']}/Protein_Intensity.parquet"

    assert UUID(result["tables"][1]["id"], version=4) is not None
    assert result["tables"][1]["name"] == "Protein_Metadata"
    assert result["tables"][1]["path"] == f"job_runs/{result['run_id']}/Protein_Metadata.parquet"

    assert UUID(result["tables"][2]["id"], version=4) is not None
    assert result["tables"][2]["name"] == "Protein_RuntimeMetadata"
    assert result["tables"][2]["path"] == f"job_runs/{result['run_id']}/Protein_RuntimeMetadata.parquet"

    assert UUID(result["tables"][3]["id"], version=4) is not None
    assert result["tables"][3]["name"] == "Peptide_Intensity"
    assert result["tables"][3]["path"] == f"job_runs/{result['run_id']}/Peptide_Intensity.parquet"

    assert UUID(result["tables"][4]["id"], version=4) is not None
    assert result["tables"][4]["name"] == "Peptide_Metadata"
    assert result["tables"][4]["path"] == f"job_runs/{result['run_id']}/Peptide_Metadata.parquet"

@md_py
def run_process_legacy(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    metadata_table = input_datasets[0].table(IntensityTableType.METADATA, IntensityEntity.PROTEIN)
    return {IntensityTableType.INTENSITY.value: intensity_table.data, \
            IntensityTableType.METADATA.value: metadata_table.data}

def test_run_process_sets_type(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    result = run_process_legacy(input_datasets, test_params, DatasetType.INTENSITY)
    assert result["type"] == DatasetType.INTENSITY

def test_run_process_has_tables(input_datasets: list[IntensityInputDataset], test_params: TestBlahParams, \
        fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    result = run_process_legacy(input_datasets, test_params, DatasetType.INTENSITY)
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][1]["name"] == "Protein_Metadata"

@md_py
def run_process_legacy_missing_metadata(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    return {IntensityTableType.INTENSITY.value: intensity_table.data}

@md_converter
def run_legacy_converter_process(experiment_id: UUID, params: InputParams) -> dict: # noqa: ARG001
    intensity_table = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    metadata_table = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    runtime_metadata_table = pd.DataFrame({"col1": [7], "col2": ["m"]})
    return {IntensityTableType.INTENSITY.value: intensity_table, \
            IntensityTableType.METADATA.value: metadata_table, \
            IntensityTableType.RUNTIME_METADATA.value: runtime_metadata_table}

def test_run_legacy_md_converter_process(fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    params = EntityInputParams(dataset_name="foo", entity_type="Protein")

    result = run_legacy_converter_process(UUID("11111111-1111-1111-1111-111111111111"), params)

    assert UUID(result["tables"][0]["id"], version=4) is not None
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][0]["path"] == f"job_runs/{result['run_id']}/intensity.parquet"

    assert UUID(result["tables"][1]["id"], version=4) is not None
    assert result["tables"][1]["name"] == "Protein_Metadata"
    assert result["tables"][1]["path"] == f"job_runs/{result['run_id']}/metadata.parquet"

def test_run_legacy_md_converter_process_with_peptide(fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    params = EntityInputParams(dataset_name="foo", entity_type="Peptide")

    result = run_legacy_converter_process(UUID("11111111-1111-1111-1111-111111111111"), params)

    assert UUID(result["tables"][0]["id"], version=4) is not None
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][0]["path"] == f"job_runs/{result['run_id']}/intensity.parquet"

    assert UUID(result["tables"][1]["id"], version=4) is not None
    assert result["tables"][1]["name"] == "Protein_Metadata"
    assert result["tables"][1]["path"] == f"job_runs/{result['run_id']}/metadata.parquet"

    assert UUID(result["tables"][2]["id"], version=4) is not None
    assert result["tables"][2]["name"] == "Protein_RuntimeMetadata"
    assert result["tables"][2]["path"] == f"job_runs/{result['run_id']}/runtime_metadata.parquet"


@md_py
def run_process_empty_tables(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    return [
        IntensityData(
            entity=IntensityEntity.PROTEIN,
            tables=[],  # Empty tables list
        ),
    ]


def test_run_process_empty_tables(input_datasets: list[IntensityInputDataset], \
        test_params: TestBlahParams, fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with pytest.raises(Exception) as exception_info:
        assert run_process_empty_tables(input_datasets, test_params, DatasetType.INTENSITY).is_failed()

    assert "The field 'intensity' must be set and cannot be None." in str(exception_info.value)


@md_py
def run_process_missing_intensity(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    return [
        IntensityData(
            entity=IntensityEntity.PROTEIN,
            tables=[
                IntensityTable(type=IntensityTableType.METADATA, data=intensity_table.data),
            ],
        ),
    ]


def test_run_process_missing_intensity(input_datasets: list[IntensityInputDataset], \
        test_params: TestBlahParams, fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with pytest.raises(Exception) as exception_info:
        assert run_process_missing_intensity(input_datasets, test_params, DatasetType.INTENSITY).is_failed()

    assert "The field 'intensity' must be set and cannot be None." in str(exception_info.value)


@md_py
def run_process_duplicate_table_types(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    metadata_table = input_datasets[0].table(IntensityTableType.METADATA, IntensityEntity.PROTEIN)
    return [
        IntensityData(
            entity=IntensityEntity.PROTEIN,
            tables=[
                IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table.data),
                IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table.data),
                IntensityTable(type=IntensityTableType.METADATA, data=metadata_table.data),
            ],
        ),
    ]


def test_run_process_duplicate_table_types(input_datasets: list[IntensityInputDataset], \
        test_params: TestBlahParams, fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    result = run_process_duplicate_table_types(input_datasets, test_params, DatasetType.INTENSITY)

    # We currently allow this, but we likely should throw a validation error
    assert len(result["tables"]) == THREE_EXPECTED_TABLES_COUNT
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][1]["name"] == "Protein_Intensity"
    assert result["tables"][2]["name"] == "Protein_Metadata"


@md_py
def run_process_none_data(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    return [
        IntensityData(
            entity=IntensityEntity.PROTEIN,
            tables=[
                IntensityTable(type=IntensityTableType.INTENSITY, data=None),
                IntensityTable(type=IntensityTableType.METADATA, data=pd.DataFrame({"col1": [1]})),
            ],
        ),
    ]


def test_run_process_none_data(input_datasets: list[IntensityInputDataset], \
        test_params: TestBlahParams, fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    fake_file_manager.load_parquet_to_df.return_value = test_data

    with pytest.raises(ValidationError) as exception_info:
        assert run_process_none_data(input_datasets, test_params, DatasetType.INTENSITY).is_failed()

    assert "Input should be an instance of DataFrame" in str(exception_info.value)


@md_py
def run_process_different_entity_types(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    metadata_table = input_datasets[0].table(IntensityTableType.METADATA, IntensityEntity.PROTEIN)
    return [
        IntensityData(
            entity=IntensityEntity.PROTEIN,
            tables=[
                IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table.data),
                IntensityTable(type=IntensityTableType.METADATA, data=metadata_table.data),
            ],
        ),
        IntensityData(
            entity=IntensityEntity.PEPTIDE,
            tables=[
                IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table.data),
                IntensityTable(type=IntensityTableType.METADATA, data=metadata_table.data),
            ],
        ),
    ]


def test_run_process_different_entity_types(input_datasets: list[IntensityInputDataset], \
        test_params: TestBlahParams, fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    result = run_process_different_entity_types(input_datasets, test_params, DatasetType.INTENSITY)

    assert len(result["tables"]) == FOUR_EXPECTED_TABLES_COUNT
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][1]["name"] == "Protein_Metadata"
    assert result["tables"][2]["name"] == "Peptide_Intensity"
    assert result["tables"][3]["name"] == "Peptide_Metadata"

    assert "Protein_Intensity.parquet" in result["tables"][0]["path"]
    assert "Protein_Metadata.parquet" in result["tables"][1]["path"]
    assert "Peptide_Intensity.parquet" in result["tables"][2]["path"]
    assert "Peptide_Metadata.parquet" in result["tables"][3]["path"]


@md_py
def run_process_with_runtime_metadata(input_datasets: list[IntensityInputDataset], params: InputParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001

    intensity_table = input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN)
    metadata_table = input_datasets[0].table(IntensityTableType.METADATA, IntensityEntity.PROTEIN)
    return [
        IntensityData(
            entity=IntensityEntity.PROTEIN,
            tables=[
                IntensityTable(type=IntensityTableType.INTENSITY, data=intensity_table.data),
                IntensityTable(type=IntensityTableType.METADATA, data=metadata_table.data),
                IntensityTable(type=IntensityTableType.RUNTIME_METADATA, data=pd.DataFrame({"runtime": [1, 2, 3]})),
            ],
        ),
    ]


def test_run_process_with_runtime_metadata(input_datasets: list[IntensityInputDataset], \
        test_params: TestBlahParams, fake_file_manager: FileManager):
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    test_metadata = pd.DataFrame({"col1": [4, 5, 6], "col2": ["x", "y", "z"]})
    fake_file_manager.load_parquet_to_df.side_effect = [test_data, test_metadata]

    result = run_process_with_runtime_metadata(input_datasets, test_params, DatasetType.INTENSITY)

    assert len(result["tables"]) == THREE_EXPECTED_TABLES_COUNT
    assert result["tables"][0]["name"] == "Protein_Intensity"
    assert result["tables"][1]["name"] == "Protein_Metadata"
    assert result["tables"][2]["name"] == "Protein_RuntimeMetadata"

    assert "Protein_RuntimeMetadata.parquet" in result["tables"][2]["path"]


# IntensityDataset dump()

def test_intensity_dataset_dump_caching():
    """Test that the dump method properly caches results."""
    from md_dataset.models.dataset import IntensityDataset

    dataset = IntensityDataset(
        run_id=UUID("11111111-1111-1111-1111-111111111111"),
        dataset_type=DatasetType.INTENSITY,
        intensity_tables=[
            IntensityData(
                entity=IntensityEntity.PROTEIN,
                tables=[
                    IntensityTable(type=IntensityTableType.INTENSITY, data=pd.DataFrame({"col1": [1, 2, 3]})),
                    IntensityTable(type=IntensityTableType.METADATA, data=pd.DataFrame({"col2": ["a", "b", "c"]})),
                ],
            ),
        ],
    )

    result1 = dataset.dump()
    result2 = dataset.dump()

    assert result1 is result2

    assert result1["type"] == DatasetType.INTENSITY
    assert result1["run_id"] == UUID("11111111-1111-1111-1111-111111111111")
    assert len(result1["tables"]) == 2


def test_intensity_dataset_dump_table_ordering():
    """Test that tables are ordered consistently in dump output."""
    from md_dataset.models.dataset import IntensityDataset

    dataset = IntensityDataset(
        run_id=UUID("11111111-1111-1111-1111-111111111111"),
        dataset_type=DatasetType.INTENSITY,
        intensity_tables=[
            IntensityData(
                entity=IntensityEntity.PEPTIDE,
                tables=[
                    IntensityTable(type=IntensityTableType.INTENSITY, data=pd.DataFrame({"col1": [1, 2, 3]})),
                    IntensityTable(type=IntensityTableType.METADATA, data=pd.DataFrame({"col2": ["a", "b", "c"]})),
                ],
            ),
            IntensityData(
                entity=IntensityEntity.PROTEIN,
                tables=[
                    IntensityTable(type=IntensityTableType.INTENSITY, data=pd.DataFrame({"col1": [4, 5, 6]})),
                    IntensityTable(type=IntensityTableType.METADATA, data=pd.DataFrame({"col2": ["x", "y", "z"]})),
                ],
            ),
        ],
    )

    result = dataset.dump()

    expected_order = [
        "Peptide_Intensity",
        "Peptide_Metadata",
        "Protein_Intensity",
        "Protein_Metadata",
    ]

    actual_order = [table["name"] for table in result["tables"]]
    assert actual_order == expected_order


def test_intensity_dataset_dump_uuid_uniqueness():
    from md_dataset.models.dataset import IntensityDataset

    dataset = IntensityDataset(
        run_id=UUID("11111111-1111-1111-1111-111111111111"),
        dataset_type=DatasetType.INTENSITY,
        intensity_tables=[
            IntensityData(
                entity=IntensityEntity.PROTEIN,
                tables=[
                    IntensityTable(type=IntensityTableType.INTENSITY, data=pd.DataFrame({"col1": [1, 2, 3]})),
                    IntensityTable(type=IntensityTableType.METADATA, data=pd.DataFrame({"col2": ["a", "b", "c"]})),
                    IntensityTable(type=IntensityTableType.RUNTIME_METADATA, data=pd.DataFrame({"col3": [7, 8, 9]})),
                ],
            ),
        ],
    )

    result = dataset.dump()

    uuids = [table["id"] for table in result["tables"]]
    assert len(uuids) == len(set(uuids))

    for uuid_str in uuids:
        UUID(uuid_str, version=4)


def test_intensity_dataset_dump_path_generation():
    from md_dataset.models.dataset import IntensityDataset

    dataset = IntensityDataset(
        run_id=UUID("11111111-1111-1111-1111-111111111111"),
        dataset_type=DatasetType.INTENSITY,
        intensity_tables=[
            IntensityData(
                entity=IntensityEntity.PROTEIN,
                tables=[
                    IntensityTable(type=IntensityTableType.INTENSITY, data=pd.DataFrame({"col1": [1, 2, 3]})),
                    IntensityTable(type=IntensityTableType.METADATA, data=pd.DataFrame({"col2": ["a", "b", "c"]})),
                    IntensityTable(type=IntensityTableType.RUNTIME_METADATA, data=pd.DataFrame({"col3": [7, 8, 9]})),
                ],
            ),
        ],
    )

    result = dataset.dump()

    expected_paths = [
        "job_runs/11111111-1111-1111-1111-111111111111/Protein_Intensity.parquet",
        "job_runs/11111111-1111-1111-1111-111111111111/Protein_Metadata.parquet",
        "job_runs/11111111-1111-1111-1111-111111111111/Protein_RuntimeMetadata.parquet",
    ]

    actual_paths = [table["path"] for table in result["tables"]]
    assert actual_paths == expected_paths
