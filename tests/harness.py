from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd
from prefect.testing.utilities import prefect_test_harness


@contextmanager
def md_test_harness():
    saved: dict[str, pd.DataFrame] = {}
    mock_fm = MagicMock()

    def capture_save_tables(tables: list[tuple[str, pd.DataFrame]]) -> None:
        for path, df in tables:
            saved[path] = df
            print(f"\n--- {path} ---")
            print(df.head())

    mock_fm.save_tables.side_effect = capture_save_tables

    with prefect_test_harness():
        with patch("md_dataset.process.get_file_manager", return_value=mock_fm):
            yield mock_fm, saved
