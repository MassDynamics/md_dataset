import pandas as pd
import pytest
from tools.harness import md_dataset_test_harness
import md_dataset.process
from md_dataset.storage import FileManager


@pytest.fixture
def sample_tables() -> list[tuple[str, pd.DataFrame]]:
    return [
        ("job_runs/abc/Protein_Intensity.parquet", pd.DataFrame({"col1": [1, 2, 3]})),
        ("job_runs/abc/Protein_Metadata.parquet", pd.DataFrame({"col2": ["a", "b", "c"]})),
    ]


def test_file_manager_is_patched():
    with md_dataset_test_harness() as (file_manager, _):
        assert md_dataset.process.get_file_manager() is file_manager


def test_file_manager_has_file_manager_spec():
    with md_dataset_test_harness() as (file_manager, _):
        assert isinstance(file_manager, FileManager)


def test_save_tables_captures_into_dict(sample_tables: list[tuple[str, pd.DataFrame]]):
    with md_dataset_test_harness() as (file_manager, saved_tables):
        file_manager.save_tables(sample_tables)

    assert len(saved_tables) == 2  # noqa: PLR2004
    assert "job_runs/abc/Protein_Intensity.parquet" in saved_tables
    assert "job_runs/abc/Protein_Metadata.parquet" in saved_tables
    pd.testing.assert_frame_equal(saved_tables["job_runs/abc/Protein_Intensity.parquet"], sample_tables[0][1])
    pd.testing.assert_frame_equal(saved_tables["job_runs/abc/Protein_Metadata.parquet"], sample_tables[1][1])


def test_save_tables_prints_head(capsys: pytest.CaptureFixture[str], sample_tables: list[tuple[str, pd.DataFrame]]):
    with md_dataset_test_harness() as (file_manager, _):
        file_manager.save_tables(sample_tables)

    output = capsys.readouterr().out
    assert "job_runs/abc/Protein_Intensity.parquet" in output
    assert "job_runs/abc/Protein_Metadata.parquet" in output
