from contextlib import contextmanager
from unittest.mock import MagicMock
from unittest.mock import patch
import pandas as pd
from prefect.testing.utilities import prefect_test_harness
from md_dataset.storage import FileManager


@contextmanager
def md_dataset_test_harness():
    saved: dict[str, pd.DataFrame] = {}
    mock_fm = MagicMock(spec=FileManager)

    def capture_save_tables(tables: list[tuple[str, pd.DataFrame]]) -> None:
        for path, df in tables:
            saved[path] = df
            print(f"\n--- {path} ---")
            print(df.head())

    mock_fm.save_tables.side_effect = capture_save_tables

    with prefect_test_harness(), patch("md_dataset.process.get_file_manager", return_value=mock_fm):
        yield mock_fm, saved
