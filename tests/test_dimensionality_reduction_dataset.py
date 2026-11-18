"""Unit tests for DimensionalityReductionDataset's output contract.

Same table names for every method and axis. `scores` and `runtime_metadata` are required;
`loadings`, `variance_explained`, `metadata_association` and `qc` are persisted only when supplied.
"""

from uuid import uuid4
import pandas as pd
import pytest
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import DimensionalityReductionDataset
from md_dataset.models.dataset import DimensionalityReductionTableType
from md_dataset.models.factory import create_dataset_from_run

ALL_TABLES = ["scores", "loadings", "variance_explained", "metadata_association", "qc", "runtime_metadata"]


def _frame() -> pd.DataFrame:
    return pd.DataFrame({"a": [1.0, 2.0]})


def _names(ds: DimensionalityReductionDataset) -> list[str]:
    return [path.split("/")[-1].removesuffix(".parquet") for path, _ in ds.tables()]


def test_table_type_values():
    assert [t.value for t in DimensionalityReductionTableType] == ALL_TABLES
    assert DatasetType.DIMENSIONALITY_REDUCTION.value == "DIMENSIONALITY_REDUCTION"


def test_minimal_run_emits_only_required_tables():
    """E.g. UMAP / t-SNE on the feature axis with QC off."""
    ds = DimensionalityReductionDataset(
        run_id=uuid4(), dataset_type=DatasetType.DIMENSIONALITY_REDUCTION,
        scores=_frame(), runtime_metadata=_frame(),
    )
    assert _names(ds) == ["scores", "runtime_metadata"]
    assert [t["name"] for t in ds.dump()["tables"]] == ["scores", "runtime_metadata"]


def test_pca_sample_axis_emits_all_tables_in_fixed_order():
    run_id = uuid4()
    ds = DimensionalityReductionDataset(
        run_id=run_id, dataset_type=DatasetType.DIMENSIONALITY_REDUCTION,
        **{name: _frame() for name in ALL_TABLES},
    )
    assert _names(ds) == ALL_TABLES
    dump = ds.dump()
    assert dump["type"] == DatasetType.DIMENSIONALITY_REDUCTION
    assert dump["run_id"] == run_id
    assert [t["name"] for t in dump["tables"]] == ALL_TABLES
    assert all(t["path"] == f"job_runs/{run_id}/{t['name']}.parquet" for t in dump["tables"])


@pytest.mark.parametrize("missing", ["scores", "runtime_metadata"])
def test_required_tables(missing: str):
    tables = {"scores": _frame(), "runtime_metadata": _frame()}
    tables.pop(missing)
    with pytest.raises(ValueError, match=missing):
        DimensionalityReductionDataset(run_id=uuid4(), dataset_type=DatasetType.DIMENSIONALITY_REDUCTION, **tables)


def test_optional_must_be_a_dataframe_if_provided():
    with pytest.raises(TypeError, match="loadings"):
        DimensionalityReductionDataset(
            run_id=uuid4(), dataset_type=DatasetType.DIMENSIONALITY_REDUCTION,
            scores=_frame(), runtime_metadata=_frame(), loadings="not a dataframe",
        )


def test_registered_in_factory():
    ds = create_dataset_from_run(
        uuid4(), DatasetType.DIMENSIONALITY_REDUCTION,
        {"scores": _frame(), "runtime_metadata": _frame(), "qc": _frame()},
    )
    assert isinstance(ds, DimensionalityReductionDataset)
    assert _names(ds) == ["scores", "qc", "runtime_metadata"]
