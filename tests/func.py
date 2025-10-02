from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import InputDataset
from md_dataset.models.dataset import InputParams
from md_dataset.process import md_py


class TestBlahParams(InputParams):
    id: int

@md_py
def test_func(input_datasets: list[InputDataset], params: TestBlahParams, \
        output_dataset_type: DatasetType) -> dict: # noqa: ARG001
    """A nice description."""
    return {}

@md_py
def test_func_properties(input_datasets: list[InputDataset], params: TestBlahParams, \
        output_dataset_type: DatasetType) -> dict:
    """A nice description."""
    return {}
