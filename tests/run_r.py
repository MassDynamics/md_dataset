import pandas as pd
from pathlib import Path
from uuid import UUID
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import InputDatasetTable
from md_dataset.models.dataset import InputParams
from md_dataset.models.dataset import IntensityEntity
from md_dataset.models.dataset import IntensityInputDataset
from md_dataset.models.dataset import IntensityTableType
from md_dataset.models.r import RFuncArgs
from md_dataset.process import md_r
from harness import md_test_harness


TEST_DATA_DIR = Path(__file__).parent / "data" / "abcd1234"


class TestRParams(InputParams):
    message: str


def input_datasets() -> list[IntensityInputDataset]:
    return [IntensityInputDataset(id=UUID("f3127c62-e0a8-4b48-9bc2-e40eb821aab1"), name="interesting name", tables=[
        InputDatasetTable(name="Protein_Intensity"),
        InputDatasetTable(name="Protein_Metadata"),
        ])]

@md_r(r_file="./tests/test_process.r", r_function="process_legacy")
def prepare_test_run_r_legacy(input_datasets: list[IntensityInputDataset], params: TestRParams, \
        output_dataset_type: DatasetType) -> RFuncArgs: # noqa: ARG001
    return RFuncArgs(data_frames = [ \
            input_datasets[0].table(IntensityTableType.INTENSITY, IntensityEntity.PROTEIN).data, \
            input_datasets[0].table(IntensityTableType.METADATA, IntensityEntity.PROTEIN).data], \
            r_args=[params.message])

if __name__ == "__main__":
    intensity_data = pd.read_parquet(TEST_DATA_DIR / "Protein_Intensity.parquet")
    metadata_data = pd.read_parquet(TEST_DATA_DIR / "Protein_Metadata.parquet")

    with md_test_harness() as (file_manager, saved_tables):
        file_manager.load_parquet_to_df.side_effect = [intensity_data, metadata_data]
        prepare_test_run_r_legacy(input_datasets(), TestRParams(message="hello"), DatasetType.INTENSITY)

    print(saved_tables.keys())
