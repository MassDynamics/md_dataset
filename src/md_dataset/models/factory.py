"""Dataset factory for creating dataset instances based on type and structure."""

from uuid import UUID
from md_dataset.models.dataset import AnovaDataset
from md_dataset.models.dataset import Dataset
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import DoseResponseDataset
from md_dataset.models.dataset import IntensityDataset
from md_dataset.models.dataset import LegacyIntensityDataset
from md_dataset.models.dataset import PairwiseDataset

_DATASET_REGISTRY = {
    (DatasetType.INTENSITY, dict): lambda run_id, dataset_type, \
            tables: LegacyIntensityDataset(run_id=run_id, dataset_type=dataset_type, **tables),
    (DatasetType.INTENSITY, list): lambda run_id, dataset_type, \
            tables: IntensityDataset(run_id=run_id, dataset_type=dataset_type, intensity_tables=tables),
    (DatasetType.PAIRWISE, dict): lambda run_id, dataset_type, \
            tables: PairwiseDataset(run_id=run_id, dataset_type=dataset_type, **tables),
    (DatasetType.ANOVA, dict): lambda run_id, dataset_type, \
            tables: AnovaDataset(run_id=run_id, dataset_type=dataset_type, **tables),
    (DatasetType.DOSE_RESPONSE, dict): lambda run_id, dataset_type, \
            tables: DoseResponseDataset(run_id=run_id, dataset_type=dataset_type, **tables),
}


def create_dataset_from_run(run_id: UUID, dataset_type: DatasetType, tables: list | dict) -> Dataset:
    """Create a dataset instance based on type and tables structure.

    Args:
        run_id: The run ID for the dataset
        dataset_type: The type of dataset to create
        tables: The table data (dict for legacy formats, list for new formats)

    Returns:
        A dataset instance of the appropriate type

    Raises:
        ValueError: If the dataset type and table structure combination is not supported
    """
    table_type = type(tables)
    factory_func = _DATASET_REGISTRY.get((dataset_type, table_type))

    if factory_func is None:
        msg = f"Unsupported dataset type: {dataset_type} with table type: {table_type.__name__}"
        raise ValueError(msg)

    return factory_func(run_id, dataset_type, tables)
