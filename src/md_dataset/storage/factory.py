"""Storage factory utilities."""

import os
from md_dataset.storage.file_manager import FileManager
from md_dataset.storage.reference_data_manager import ReferenceDataManager
from md_dataset.storage.s3 import get_s3_client


def get_file_manager() -> FileManager:
    """Get file manager for storage operations."""
    return FileManager(client=get_s3_client(), default_bucket=os.getenv("RESULTS_BUCKET"))


def get_reference_data_manager() -> ReferenceDataManager:
    """Get reference data manager for loading shared reference data."""
    return ReferenceDataManager()
