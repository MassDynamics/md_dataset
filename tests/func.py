from md_dataset.models.types import BiomolecularSource
from md_dataset.models.types import DatasetType
from md_dataset.models.types import InputDataset
from md_dataset.models.types import InputParams
from md_dataset.process import md_py


class TestBlahParams(InputParams):
    id: int
    source: BiomolecularSource

@md_py
def test_func(input_datasets: list[InputDataset], params: TestBlahParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001
    return {}
