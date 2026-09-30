"""Round-trip tests for the TIME_COURSE dataset type."""

import io
import uuid
import numpy as np
import pandas as pd
import pytest
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import TimeCourseDataset
from md_dataset.models.factory import create_dataset_from_run


def _stats() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "GroupId": ["1", "2", "1", "2", "1", "2"],
            "test": ["along_x", "along_x", "along_x", "along_x", "trend_difference", "trend_difference"],
            "group": ["ctrl", "ctrl", "treated", "treated", None, None],
            "F": [3.2, np.nan, 1.1, 0.4, 2.5, np.nan],
            "df1": [3.0] * 6,
            "df_residual": [34.0, 0.0, 34.0, 0.0, 34.0, 0.0],
            "df_prior": [4.15, 4.15, 4.15, 4.15, 3.8, 3.8],
            "AveExpr": [21.7, 22.0, 21.7, 22.0, 21.7, 22.0],
            "P.Value": [0.03, np.nan, 0.35, 0.75, 0.04, np.nan],
            "adj.P.Val": [0.06, np.nan, 0.7, 0.75, 0.04, np.nan],
            "ProteinIds": ["P1", "P2", "P1", "P2", "P1", "P2"],
        },
    )


def _curves() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "GroupId": ["1", "1", "2", "2"],
            "group": ["ctrl"] * 4,
            "x": [0.0, 24.0, 0.0, 24.0],
            "fitted_log2": [21.4, 22.1, np.nan, np.nan],
        },
    )


def _roundtrip(df: pd.DataFrame) -> pd.DataFrame:
    """Through parquet with the options FileManager.save_df_to_parquet uses."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="gzip", index=False, row_group_size=16_000)
    return pd.read_parquet(io.BytesIO(buf.getvalue()), engine="pyarrow")


def test_factory_creates_time_course_and_tables_round_trip():
    run_id = uuid.uuid4()
    meta = pd.DataFrame({"spline_df": [3], "limma_core_rev": ["e3991af"]})
    tables = {"stats": _stats(), "curves": _curves(), "runtime_metadata": meta}
    ds = create_dataset_from_run(run_id, DatasetType.TIME_COURSE, tables)
    assert isinstance(ds, TimeCourseDataset)

    dump = ds.dump()
    assert dump["type"] == DatasetType.TIME_COURSE
    assert [t["name"] for t in dump["tables"]] == ["stats", "curves", "runtime_metadata"]
    for t in dump["tables"]:
        assert t["path"] == f"job_runs/{run_id}/{t['name']}.parquet"
    assert [path for path, _ in ds.tables()] == [t["path"] for t in dump["tables"]]

    for (_, df), original in zip(ds.tables(), tables.values(), strict=True):
        pd.testing.assert_frame_equal(_roundtrip(df), original)


def test_runtime_metadata_is_optional():
    ds = TimeCourseDataset(
        run_id=uuid.uuid4(), dataset_type=DatasetType.TIME_COURSE, stats=_stats(), curves=_curves(),
    )
    assert [t["name"] for t in ds.dump()["tables"]] == ["stats", "curves"]


def test_dump_is_cached():
    ds = TimeCourseDataset(
        run_id=uuid.uuid4(), dataset_type=DatasetType.TIME_COURSE, stats=_stats(), curves=_curves(),
    )
    assert ds.dump() is ds.dump()


def test_missing_curves_raises():
    with pytest.raises(ValueError, match="curves"):
        TimeCourseDataset(run_id=uuid.uuid4(), dataset_type=DatasetType.TIME_COURSE, stats=_stats())


def test_stats_must_be_a_dataframe():
    with pytest.raises(TypeError, match="stats"):
        TimeCourseDataset(
            run_id=uuid.uuid4(), dataset_type=DatasetType.TIME_COURSE, stats="nope", curves=_curves(),
        )
