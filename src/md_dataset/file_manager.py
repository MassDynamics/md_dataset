from __future__ import annotations
import io
import logging
from io import BytesIO
from typing import TYPE_CHECKING
import botocore
import pandas as pd

if TYPE_CHECKING:
    from types import TracebackType
    from boto3_type_annotations.s3 import Client
    from src.models.table import PandasDataFrame

logger = logging.getLogger(__name__)


class FileManager:
    def __init__(self, client: Client):
        self.client = client

    class Downloader:
        def __init__(self, client: Client, bucket: str, path: str):
            self.client = client
            self.bucket = bucket
            self.path = path

        def __enter__(self):
            bio = BytesIO()
            try:
                logger.debug("Download: %s", self.path)
                self.client.download_fileobj(self.bucket, self.path, bio)
                return bio.getvalue()
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return False
                raise

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
        ):
            logger.debug("exit")

    def _file_download(self, bucket: str, path: str) -> BytesIO:
        return FileManager.Downloader(self.client, bucket, path)

    def load_parquet_to_df(self, bucket: str, path: str) -> PandasDataFrame:
        with self._file_download(bucket, path) as content:
            logging.debug("load_parquet_to_df: %s", self.path)
            return pd.read_parquet(io.BytesIO(content), engine="pyarrow")
