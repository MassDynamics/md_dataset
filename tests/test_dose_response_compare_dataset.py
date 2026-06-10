"""Round-trip tests for the DOSE_RESPONSE_COMPARE dataset type."""

import uuid
import pandas as pd
import pytest
from md_dataset.models.dataset import DatasetType
from md_dataset.models.dataset import DoseResponseCompareDataset
from md_dataset.models.factory import create_dataset_from_run


def _df(**cols: object) -> pd.DataFrame:
    return pd.DataFrame(cols)


def _comparisons() -> pd.DataFrame:
    return _df(GroupId=[1, 2], **{"MaxLog2FoldChange A - B": [1.0, -2.0]})


def _curves() -> pd.DataFrame:
    return _df(GroupId=[1, 1], Dose=[0.1, 1.0], DrcPred=[5.0, 6.0])


def _make(**extra: pd.DataFrame) -> DoseResponseCompareDataset:
    return DoseResponseCompareDataset(
        run_id=uuid.uuid4(),
        dataset_type=DatasetType.DOSE_RESPONSE_COMPARE,
        output_comparisons=_comparisons(),
        output_curves=_curves(),
        **extra,
    )


def test_dump_table_names_and_paths():
    ds = _make(runtime_metadata=_df(FitMethod=["loess"]))
    dump = ds.dump()
    assert dump["type"] == DatasetType.DOSE_RESPONSE_COMPARE
    names = [t["name"] for t in dump["tables"]]
    assert names == ["output_comparisons", "output_curves", "runtime_metadata"]
    for t in dump["tables"]:
        assert t["path"] == f"job_runs/{ds.run_id}/{t['name']}.parquet"


def test_dump_is_cached():
    ds = _make()
    assert ds.dump() is ds.dump()


def test_input_drc_omitted_by_default():
    ds = _make()
    assert [n for n, _ in ds.tables()].count(f"job_runs/{ds.run_id}/input_drc.parquet") == 0
    assert [t["name"] for t in ds.dump()["tables"]] == ["output_comparisons", "output_curves"]


def test_input_drc_included_when_provided():
    ds = _make(input_drc=_df(GroupId=[1], replicate=["s1"], InitialIntensity=[5.0]))
    assert [t["name"] for t in ds.dump()["tables"]] == [
        "output_comparisons",
        "output_curves",
        "input_drc",
    ]


def test_factory_creates_dose_response_compare():
    run_id = uuid.uuid4()
    tables = {"output_comparisons": _comparisons(), "output_curves": _curves()}
    ds = create_dataset_from_run(run_id, DatasetType.DOSE_RESPONSE_COMPARE, tables)
    assert isinstance(ds, DoseResponseCompareDataset)
    assert [t["name"] for t in ds.dump()["tables"]] == ["output_comparisons", "output_curves"]


def test_missing_required_output_curves_raises():
    with pytest.raises(ValueError, match="output_curves"):
        DoseResponseCompareDataset(
            run_id=uuid.uuid4(),
            dataset_type=DatasetType.DOSE_RESPONSE_COMPARE,
            output_comparisons=_comparisons(),
        )
