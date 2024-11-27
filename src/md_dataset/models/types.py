from __future__ import annotations
from enum import Enum
from typing import TYPE_CHECKING
from typing import TypeVar
import pandas as pd
from pydantic import BaseModel
from pydantic import conlist

if TYPE_CHECKING:
    from md_dataset.file_manager import FileManager

pd.core.frame.PandasDataFrame = TypeVar("pd.core.frame.DataFrame")

class DatasetType(Enum):
    INTENSITY = "INTENSITY"
    DOSE_RESPONSE = "DOSE_RESPONSE"

class InputParams(BaseModel):
  """The name of the dataset.

  Keyword arguments:
  dataset_name: the name of the OutputDataset to create
  dataset_type: the name of the OutputDataset to create

  If dataset_name or dataset_type are not set a default will be used.
  """
  dataset_name: str | None
  dataset_type: DatasetType | None

class IntensityInputParams(InputParams):
    type: DatasetType = DatasetType.INTENSITY

class DoseResponseInputParams(InputParams):
    type: DatasetType = DatasetType.DOSE_RESPONSE

class InputDatasetTable(BaseModel):
    name: str
    bucket: str = None
    key: str = None
    data: pd.core.frame.PandasDataFrame = None

class InputDataset(BaseModel):
    name: str
    type: DatasetType
    tables: list[InputDatasetTable]

    def table_by_name(self, name: str) -> InputDatasetTable:
        return next(filter(lambda table: table.name == name, self.tables), None)

    def table_data_by_name(self, name: str) -> pd.core.frame.PandasDataFrame:
        return self.table_by_name(name).data

    def populate_tables(self, file_manager: FileManager) -> InputDataset:
        tables = [
                InputDatasetTable(**table.dict(exclude={"data", "bucket", "key"}), \
                        data = file_manager.load_parquet_to_df( \
                            bucket = table.bucket, key = table.key)) \
                for table in self.tables]

        return InputDataset(**self.dict(exclude={"tables"}), tables = tables)

InputDataset.update_forward_refs()

class IntensityInputDataset(InputDataset):
    type: DatasetType = DatasetType.INTENSITY

class DoseResponseInputDataset(InputDataset):
    type: DatasetType = DatasetType.DOSE_RESPONSE

class OutputDatasetTable(BaseModel):
    name: str
    data: pd.core.frame.PandasDataFrame

class OutputDataset(BaseModel):
    name: str
    type: DatasetType
    tables: list[OutputDatasetTable]

# DEPRECATED (from another project)
class FlowOutPutTable(BaseModel):
    name: str
    data: pd.core.frame.PandasDataFrame


# DEPRECATED (from another project)
class FlowOutPutDataSet(BaseModel):
    name: str
    tables: list[FlowOutPutTable]
    type: DatasetType


# DEPRECATED (from another project)
class FlowOutPut(BaseModel):
    data_sets: conlist(FlowOutPutDataSet, min_items=1, max_items=1)

    def data(self, i: int) -> list:
        return self.data_sets[0].tables[i].data

FlowOutPut.update_forward_refs()

class RPreparation(BaseModel):
    data_frames: list[pd.core.frame.PandasDataFrame]
    r_args: list[str]
