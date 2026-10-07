"""Round-trip tests for the CAMERA_PR dataset type."""

import io
import uuid
import pandas as pd
import pytest
from md_dataset.models.dataset import CameraPRDataset
from md_dataset.models.dataset import DatasetType
from md_dataset.models.factory import create_dataset_from_run


def _results() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "database": ["Reactome", "Reactome"],
            "id": ["R-1", "R-2"],
            "NGenes": [30, 45],
            "Direction": ["Up", "Down"],
            "PValue": [0.01, 0.2],
            "FDR": [0.04, 0.3],
        },
    )


def _roundtrip(df: pd.DataFrame) -> pd.DataFrame:
    """Through parquet with the options FileManager.save_df_to_parquet uses."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="gzip", index=False, row_group_size=16_000)
    return pd.read_parquet(io.BytesIO(buf.getvalue()), engine="pyarrow")


def test_factory_creates_camera_pr_and_tables_round_trip():
    run_id = uuid.uuid4()
    tables = {
        "results": _results(),
        "runtime_metadata": pd.DataFrame({"enrichmentMethod": ["cameraPR"]}),
        "database_metadata": pd.DataFrame({"id": ["R-1"], "name": ["Pathway"]}),
    }
    ds = create_dataset_from_run(run_id, DatasetType.CAMERA_PR, tables)
    assert isinstance(ds, CameraPRDataset)

    dump = ds.dump()
    assert dump["type"] == DatasetType.CAMERA_PR
    assert [t["name"] for t in dump["tables"]] == [
        "output_comparisons", "runtime_metadata", "database_metadata",
    ]
    assert [t["path"] for t in dump["tables"]] == [
        f"job_runs/{run_id}/{name}.parquet" for name in ("results", "runtime_metadata", "database_metadata")
    ]
    assert [path for path, _ in ds.tables()] == [t["path"] for t in dump["tables"]]

    for (_, df), original in zip(ds.tables(), tables.values(), strict=True):
        pd.testing.assert_frame_equal(_roundtrip(df), original)


def test_optional_tables_are_omitted():
    ds = CameraPRDataset(run_id=uuid.uuid4(), dataset_type=DatasetType.CAMERA_PR, results=_results())
    assert [t["name"] for t in ds.dump()["tables"]] == ["output_comparisons"]


def test_dump_is_cached():
    ds = CameraPRDataset(run_id=uuid.uuid4(), dataset_type=DatasetType.CAMERA_PR, results=_results())
    assert ds.dump() is ds.dump()


def test_missing_results_raises():
    with pytest.raises(ValueError, match="results"):
        CameraPRDataset(run_id=uuid.uuid4(), dataset_type=DatasetType.CAMERA_PR)


def test_results_must_be_a_dataframe():
    with pytest.raises(TypeError, match="results"):
        CameraPRDataset(run_id=uuid.uuid4(), dataset_type=DatasetType.CAMERA_PR, results="nope")


def test_dump_matches_enrichment_dataset():
    from md_dataset.models.dataset import EnrichmentDataset

    run_id = uuid.uuid4()
    kwargs = {
        "run_id": run_id,
        "results": _results(),
        "runtime_metadata": pd.DataFrame({"a": [1]}),
        "database_metadata": pd.DataFrame({"b": [1]}),
    }
    camera = CameraPRDataset(dataset_type=DatasetType.CAMERA_PR, **kwargs).dump()["tables"]
    enrichment = EnrichmentDataset(dataset_type=DatasetType.ENRICHMENT, **kwargs).dump()["tables"]
    assert [(t["name"], t["path"]) for t in camera] == [(t["name"], t["path"]) for t in enrichment]
