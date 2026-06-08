"""Unit tests for MOFADataset's output contract.

Focus: the optional ``factor_metadata_association`` table — present in the
persisted tables only when supplied, so an unsupervised MOFA run (no sample
metadata) keeps emitting exactly the three core tables.
"""

from uuid import uuid4
import pandas as pd
import pytest
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import MOFADataset
from md_dataset.models.dataset import MOFATableType

THREE_CORE_TABLES = 3
FOUR_TABLES_WITH_ASSOCIATION = 4


def _core_frames() -> dict:
    frame = pd.DataFrame({"a": [1.0, 2.0]})
    return {"factor_scores": frame, "factor_loadings": frame, "variance_explained": frame}


def _association_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "factor": [1, 2],
            "metadata_column": ["condition", "condition"],
            "metadata_type": ["categorical", "categorical"],
            "r2": [0.71, 0.04],
            "p_value": [0.001, 0.8],
            "test": ["anova", "anova"],
        },
    )


def test_association_table_is_an_enum_member():
    assert MOFATableType.FACTOR_METADATA_ASSOCIATION.value == "factor_metadata_association"


def test_unsupervised_run_emits_only_core_tables():
    ds = MOFADataset(run_id=uuid4(), dataset_type=DatasetType.MOFA, **_core_frames())
    names = [path.split("/")[-1].removesuffix(".parquet") for path, _ in ds.tables()]
    assert names == ["factor_scores", "factor_loadings", "variance_explained"]
    assert len(ds.tables()) == THREE_CORE_TABLES
    assert [t["name"] for t in ds.dump()["tables"]] == names


def test_association_table_persisted_when_supplied():
    ds = MOFADataset(
        run_id=uuid4(),
        dataset_type=DatasetType.MOFA,
        factor_metadata_association=_association_frame(),
        **_core_frames(),
    )
    names = [path.split("/")[-1].removesuffix(".parquet") for path, _ in ds.tables()]
    assert "factor_metadata_association" in names
    assert len(ds.tables()) == FOUR_TABLES_WITH_ASSOCIATION
    # dump() (the persisted manifest) lists it too.
    assert "factor_metadata_association" in [t["name"] for t in ds.dump()["tables"]]


def test_association_table_path_is_well_formed():
    run_id = uuid4()
    ds = MOFADataset(
        run_id=run_id,
        dataset_type=DatasetType.MOFA,
        factor_metadata_association=_association_frame(),
        **_core_frames(),
    )
    paths = {path.split("/")[-1]: path for path, _ in ds.tables()}
    assert paths["factor_metadata_association.parquet"] == f"job_runs/{run_id}/factor_metadata_association.parquet"


def test_association_must_be_a_dataframe_if_provided():
    with pytest.raises(TypeError, match="factor_metadata_association"):
        MOFADataset(
            run_id=uuid4(),
            dataset_type=DatasetType.MOFA,
            factor_metadata_association="not a dataframe",
            **_core_frames(),
        )
