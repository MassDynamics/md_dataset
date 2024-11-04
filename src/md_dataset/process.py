import os
from typing import Callable
import boto3
import pandas as pd
from md_dataset.file_manager import FileManager
from md_dataset.models.types import DatasetParams
from md_dataset.models.types import FlowOutPut
from md_dataset.models.types import FlowOutPutDataSet
from md_dataset.models.types import FlowOutPutTable

source_bucket = os.getenv("SOURCE_BUCKET")

def get_file_manager() -> None:
    client = boto3.client("s3")
    FileManager(client)

def md_process(func: Callable) -> Callable:
    def wrapper(params: DatasetParams) -> FlowOutPut:

        source_data = get_file_manager().load_parquet_to_df(bucket = source_bucket, key = params.source_key)
        results = func(source_data)

        return FlowOutPut(
            data_sets=[
                FlowOutPutDataSet(
                    name=params.name,
                    type=params.type,
                    tables=[
                        FlowOutPutTable(name="Protein_Intensity", data=pd.DataFrame(results)),
                    ],
                ),
            ],
        )

    return wrapper
