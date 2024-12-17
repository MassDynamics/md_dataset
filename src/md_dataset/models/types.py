from __future__ import annotations
import abc
import uuid
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

  Keyword Arguments:
  dataset_name: the name of the OutputDataset to create

  If dataset_name is not set a default will be used.
  """
  dataset_name: str | None

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
        self.tables = tables

class IntensityTableType(Enum):
    INTENSITY = "intensity"
    METADATA = "metadata"

class IntensityTable:
    @classmethod
    def table_name(cls, intensity_type: IntensityTableType) -> str:
        return f"Protein_{intensity_type.value.title()}"

class IntensityInputDataset(InputDataset):
    type: DatasetType = DatasetType.INTENSITY

    def table(self, intensity_type: IntensityTableType) -> InputDatasetTable:
        return next(filter(lambda table: table.name == IntensityTable.table_name(intensity_type), \
                self.tables), None)

class DoseResponseInputDataset(InputDataset):
    type: DatasetType = DatasetType.DOSE_RESPONSE

# DEPRECATED
class OutputDataset(BaseModel, abc.ABC):
    dataset_type: DatasetType
    tables: list = []

    @classmethod
    def create(cls, dataset_type: DatasetType) -> OutputDataset:
        if dataset_type == DatasetType.INTENSITY:
            return IntensityOutputDataset(dataset_type=dataset_type)
        return None


    def add(self, intensity_table_type: IntensityTableType, data: pd.core.frame.PandasDataFrame) -> None:
        self.tables.append((intensity_table_type, data))


class IntensityOutputDataset(OutputDataset):
    def dict(self) -> dict:
        return {IntensityTable.table_name(table[0]): table[1] for table in self.tables}

class Dataset(BaseModel, abc.ABC):
    run_id: uuid.UUID
    name: str
    dataset_type: DatasetType

    @abc.abstractmethod
    def tables(self) -> list:
        pass

    @classmethod
    def from_run(cls, run_id: uuid.UUID, name: str, dataset_type: DatasetType, tables: list) -> OutputDataset:
        if dataset_type == DatasetType.INTENSITY:
            return IntensityDataset(run_id=run_id, name=name, dataset_type=dataset_type, \
                    intensity=tables[0], metadata=tables[1])
        return None

class IntensityDataset(Dataset):
    intensity: pd.core.frame.PandasDataFrame
    metadata: pd.core.frame.PandasDataFrame

    def tables(self) -> list[tuple[str, pd.core.frame.PandasDataFrame]]:
        return [(self._path(IntensityTableType.INTENSITY), self.intensity), \
                (self._path(IntensityTableType.METADATA), self.metadata)]

    def dict(self) -> dict:
        return {
                "name": self.name,
                "type": self.dataset_type,
                "run_id": self.run_id,
                "tables": [
                    {
                        "name": "Protein_Intensity",
                        "path": self._path(IntensityTableType.INTENSITY),

                    },{
                        "name": "Protein_Metadata",
                        "path": self._path(IntensityTableType.METADATA),
                    },
                ],
            }

    def _path(self, table_type: IntensityTableType) -> str:
        return f"job_runs/{self.run_id}/{table_type.value}.parquet"


    def output(self) -> FlowOutPut:
        return FlowOutPutDataSet(
                name=self.name,
                type=self.dataset_type,
                tables=[
                    FlowOutPutTable(name="Protein_Intensity", data=self.intensity),
                    FlowOutPutTable(name="Protein_Metadata", data=self.metadata),
                    ],
                )


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
    datasets: conlist(FlowOutPutDataSet, min_items=1, max_items=1)

    def data(self, i: int) -> list:
        return self.datasets[0].tables[i].data

class RPreparation(BaseModel):
    data_frames: list[pd.core.frame.PandasDataFrame]
    r_args: list[str]
