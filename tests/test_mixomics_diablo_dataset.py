"""Round-trip tests for the MIXOMICS_DIABLO dataset type (MDMixOmics runDiablo output)."""

import io
import uuid
import pandas as pd
import pytest
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import MixOmicsDiabloDataset
from md_dataset.models.dataset import MixOmicsDiabloTableType
from md_dataset.models.factory import create_dataset_from_run

REQUIRED = ["sample_scores", "component_loadings", "loadings_contribution", "variance_explained", "runtime_metadata"]


def _tables() -> dict:
    return {
        "sample_scores": pd.DataFrame(
            {
                "view": ["miRNA", "weighted_average"],
                "dataset_id": ["d1", ""],
                "entity": ["Gene", ""],
                "replicate": ["S1", "S1"],
                "component": [1, 1],
                "score": [-2.1, -1.9],
                "group": ["Basal", "Basal"],
            },
        ),
        "component_loadings": pd.DataFrame(
            {
                "view": ["miRNA", "miRNA"],
                "dataset_id": ["d1", "d1"],
                "entity": ["Gene", "Gene"],
                "GroupId": [1, 2],
                "GroupLabel": ["hsa-let-7a", "hsa-let-7b"],
                "component": [1, 1],
                "loading": [0.3, 0.0],
                "correlation": [0.8, 0.1],
                "selected": [True, False],
                "center": [4.7, 9.2],
                "scale": [1.46, 0.96],
            },
        ),
        "loadings_contribution": pd.DataFrame(
            {
                "view": ["miRNA"],
                "dataset_id": ["d1"],
                "entity": ["Gene"],
                "GroupId": [1],
                "GroupLabel": ["hsa-let-7a"],
                "group": ["Basal"],
                "mean": [5.1],
                "median": [5.0],
            },
        ),
        "variance_explained": pd.DataFrame(
            {
                "view": ["miRNA"],
                "dataset_id": ["d1"],
                "entity": ["Gene"],
                "component": [1],
                "explained_variance": [0.12],
                "keep_x": [10],
                "block_weight": [0.81],
                "n_features_input": [184],
                "n_features_used": [184],
            },
        ),
        "runtime_metadata": pd.DataFrame({"method": ["DIABLO"], "n_components": [2], "ncomp_mode": ["tune"]}),
        "tuning_ncomp": pd.DataFrame(
            {
                "vote": ["WeightedVote"],
                "dist": ["centroids.dist"],
                "measure": ["Overall.BER"],
                "component": [2],
                "error_rate": [0.05],
                "error_rate_sd": [0.01],
                "chosen": [True],
            },
        ),
    }


def _roundtrip(df: pd.DataFrame) -> pd.DataFrame:
    """Through parquet with the options FileManager.save_df_to_parquet uses."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="gzip", index=False, row_group_size=16_000)
    return pd.read_parquet(io.BytesIO(buf.getvalue()), engine="pyarrow")


def _required_only() -> dict:
    return {name: df for name, df in _tables().items() if name in REQUIRED}


def test_type_value():
    assert DatasetType.MIXOMICS_DIABLO.value == "MIXOMICS_DIABLO"


def test_factory_creates_dataset_and_tables_round_trip():
    run_id = uuid.uuid4()
    tables = _tables()
    ds = create_dataset_from_run(run_id, DatasetType.MIXOMICS_DIABLO, tables)
    assert isinstance(ds, MixOmicsDiabloDataset)

    dump = ds.dump()
    assert dump["type"] == DatasetType.MIXOMICS_DIABLO
    assert [t["name"] for t in dump["tables"]] == [*REQUIRED, "tuning_ncomp"]
    assert [t["path"] for t in dump["tables"]] == [
        f"job_runs/{run_id}/{name}.parquet" for name in [*REQUIRED, "tuning_ncomp"]
    ]
    assert [path for path, _ in ds.tables()] == [t["path"] for t in dump["tables"]]

    for (_, df), name in zip(ds.tables(), [*REQUIRED, "tuning_ncomp"], strict=True):
        pd.testing.assert_frame_equal(_roundtrip(df), tables[name])


def test_table_names_are_the_enum_values():
    assert [t.value for t in MixOmicsDiabloTableType] == [*REQUIRED, "tuning_ncomp"]


def test_tuning_table_is_optional():
    ds = create_dataset_from_run(uuid.uuid4(), DatasetType.MIXOMICS_DIABLO, _required_only())
    assert [t["name"] for t in ds.dump()["tables"]] == REQUIRED
    assert len(ds.tables()) == len(REQUIRED)


def test_dump_is_cached():
    ds = MixOmicsDiabloDataset(run_id=uuid.uuid4(), dataset_type=DatasetType.MIXOMICS_DIABLO, **_required_only())
    assert ds.dump() is ds.dump()


@pytest.mark.parametrize("missing", REQUIRED)
def test_missing_required_table_raises(missing: str):
    tables = _required_only()
    del tables[missing]
    with pytest.raises(ValueError, match=missing):
        MixOmicsDiabloDataset(run_id=uuid.uuid4(), dataset_type=DatasetType.MIXOMICS_DIABLO, **tables)


def test_required_table_must_be_a_dataframe():
    tables = {**_required_only(), "sample_scores": "nope"}
    with pytest.raises(TypeError, match="sample_scores"):
        MixOmicsDiabloDataset(run_id=uuid.uuid4(), dataset_type=DatasetType.MIXOMICS_DIABLO, **tables)


def test_tuning_table_must_be_a_dataframe_if_given():
    tables = {**_required_only(), "tuning_ncomp": "nope"}
    with pytest.raises(TypeError, match="tuning_ncomp"):
        MixOmicsDiabloDataset(run_id=uuid.uuid4(), dataset_type=DatasetType.MIXOMICS_DIABLO, **tables)
