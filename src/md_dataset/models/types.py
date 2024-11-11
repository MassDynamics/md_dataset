from __future__ import annotations
from enum import Enum
from typing import TYPE_CHECKING
from typing import Any
from typing import TypeVar
import pandas as pd
from pydantic import BaseModel
from pydantic import conlist

if TYPE_CHECKING:
    from md_dataset.file_manager import FileManager

pd.core.frame.PandasDataFrame = TypeVar("pd.core.frame.DataFrame")

class DatasetInputTable(BaseModel):
    name: str
    bucket: str = None
    key: str = None
    data: pd.core.frame.PandasDataFrame = None


class DatasetInputParams(BaseModel):
    name: str
    type: DatasetType
    config: Any = None
    tables: list[DatasetInputTable]

    def table_by_name(self, name: str) -> DatasetInputTable:
        return next(filter(lambda table: table.name == name, self.tables), None)

    def table_data_by_name(self, name: str) -> pd.core.frame.PandasDataFrame:
        return self.table_by_name(name).data

    def dataset_input_params(self, file_manager: FileManager) -> DatasetInputParams:
        tables = [
                DatasetInputTable(**table.model_dump(exclude=["data", "bucket", "key"]), \
                        data = file_manager.load_parquet_to_df( \
                            bucket = table.bucket, key = table.key)) \
                for table in self.tables]

        return DatasetInputParams(**self.model_dump(exclude=["tables"]), tables = tables)

class DatasetType(Enum):
    INTENSITY = "INTENSITY"


class FlowOutPutTable(BaseModel):
    name: str
    data: pd.core.frame.PandasDataFrame


class FlowOutPutDataSet(BaseModel):
    name: str
    tables: list[FlowOutPutTable]
    type: DatasetType


class FlowOutPut(BaseModel):
    data_sets: conlist(FlowOutPutDataSet, min_length=1, max_length=1)

    def data(self, i: int) -> list:
        return self.data_sets[0].tables[i].data




