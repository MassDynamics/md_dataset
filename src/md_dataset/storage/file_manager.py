"""File management utilities for storage operations."""

from __future__ import annotations
import io
import logging
from io import BytesIO
from typing import TYPE_CHECKING
import pandas as pd

if TYPE_CHECKING:
    from types import TracebackType
    from boto3_type_annotations.s3 import Client

logger = logging.getLogger(__name__)


class FileManager:
    """File manager for handling S3 storage operations."""

    def __init__(self, client: Client, default_bucket: str):
        """Initialize file manager with S3 client and default bucket.

        Args:
            client: S3 client for storage operations
            default_bucket: Default bucket name for file operations
        """
        self.client = client
        self.default_bucket = default_bucket

    class Downloader:
        """Context manager for downloading files from S3."""

        def __init__(self, client: Client, bucket: str, key: str):
            """Initialize downloader with S3 client, bucket, and key.

            Args:
                client: S3 client for download operations
                bucket: S3 bucket name
                key: S3 object key
            """
            self.client = client
            self.bucket = bucket
            self.key = key

        def __enter__(self):
            """Download file content from S3."""
            if self.bucket is None:
                msg = "Source bucket not provided"
                raise AttributeError(msg)

            bio = BytesIO()
            logger.debug("Download: %s", self.key)
            self.client.download_fileobj(self.bucket, self.key, bio)
            return bio.getvalue()

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
        ):
            """Clean up after download operation."""
            logger.debug("exit")

    def _file_download(self, bucket: str, key: str) -> BytesIO:
        """Create a downloader context manager for a file.

        Args:
            bucket: S3 bucket name (uses default if None)
            key: S3 object key

        Returns:
            Downloader context manager
        """
        return FileManager.Downloader(self.client, bucket or self.default_bucket, key)

    def load_parquet_to_df(self, bucket: str, key: str) -> pd.DataFrame:
        """Load a parquet file from S3 into a pandas DataFrame.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Loaded pandas DataFrame
        """
        with self._file_download(bucket, key) as content:
            logging.debug("load_parquet_to_df: %s", key)
            return pd.read_parquet(io.BytesIO(content), engine="pyarrow")

    def save_tables(self, tables: list[tuple[str, pd.DataFrame]]) -> None:
        """Save multiple tables to S3 as parquet and CSV files.

        Args:
            tables: List of (path, DataFrame) tuples to save
        """
        for path, data in tables:
            self.save_df_to_parquet(path=path, df=data)
            # Also save as CSV
            csv_path = path.replace(".parquet", ".csv")
            self.save_df_to_csv(path=csv_path, df=data)

    def save_df_to_parquet(self, df: pd.DataFrame, path: str) -> None:
        """Save a pandas DataFrame to S3 as a parquet file.

        Args:
            df: DataFrame to save
            path: S3 object key for the saved file
        """
        pq_buffer = io.BytesIO()
        df.to_parquet(pq_buffer, engine="pyarrow", compression="gzip", index=False, row_group_size=16_000)
        self.client.put_object(
            Body=(pq_buffer.getvalue()),
            Bucket=self.default_bucket,
            Key=path,
        )

    def save_df_to_csv(self, df: pd.DataFrame, path: str) -> None:
        """Save a pandas DataFrame to S3 as a CSV file.

        Args:
            df: DataFrame to save
            path: S3 object key for the saved file
        """
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")
        self.client.put_object(
            Body=csv_bytes,
            Bucket=self.default_bucket,
            Key=path,
        )
