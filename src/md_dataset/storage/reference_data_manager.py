"""Reference data management utilities for storage operations."""

import logging
import os
import pandas as pd
from md_dataset.storage.file_manager import FileManager
from md_dataset.storage.s3 import get_s3_client

logger = logging.getLogger(__name__)


class ReferenceDataManager:
    """Manager for loading shared reference data from S3."""

    def __init__(self):
        """Initialize with a file manager backed by the reference data bucket."""
        self.file_manager = FileManager(
            client=get_s3_client(),
            default_bucket=os.getenv("REFERENCE_DATA_BUCKET_NAME"),
        )
        self.prefix = "reference_data/"

    def load_parquet_to_df(self, reference_data_id: str) -> pd.DataFrame:
        """Load a reference data parquet file from S3 into a pandas DataFrame.

        Args:
            reference_data_id: Identifier of the reference data to load

        Returns:
            Loaded pandas DataFrame
        """
        key = f"{self.prefix}{reference_data_id}.parquet"
        logger.debug("Download reference data: %s", key)
        return self.file_manager.load_parquet_to_df(bucket=None, key=key)
