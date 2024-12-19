from __future__ import annotations
import abc
import uuid
from enum import Enum
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import TypeVar
import pandas as pd
from pydantic import BaseModel
from pydantic import PrivateAttr
from pydantic import validator

pd.core.frame.PandasDataFrame = TypeVar("pd.core.frame.DataFrame")

if TYPE_CHECKING:
    from md_dataset.file_manager import FileManager


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

    class Config:
        arbitrary_types_allowed = True

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
    RUNTIME_METADATA = "runtime_metadata"

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

class Dataset(BaseModel, abc.ABC):
    run_id: uuid.UUID
    name: str
    dataset_type: DatasetType

    @abc.abstractmethod
    def tables(self) -> list:
        pass

    @classmethod
    def from_run(cls, run_id: uuid.UUID, name: str, dataset_type: DatasetType, tables: list) -> Dataset:
        if dataset_type == DatasetType.INTENSITY:
            return IntensityDataset(run_id=run_id, name=name, dataset_type=dataset_type, \
                    **tables)
        return None

class IntensityDataset(Dataset):
    # should match enum IntensityTableType
    intensity: pd.core.frame.PandasDataFrame
    metadata: pd.core.frame.PandasDataFrame
    runtime_metadata: pd.core.frame.PandasDataFrame = None
    _dump_cache: dict = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True  # Allow pandas DataFrame type

    @validator("intensity", "metadata", pre=True, always=True)
    def validate_dataframe(cls, value: pd.core.frame.PandasDataFrame | None, \
            field: ClassVar) -> pd.core.frame.PandasDataFrame:
        if value is None:
            msg = f"The field '{field.name}' must be set and cannot be None."
            raise ValueError(msg)
        if not isinstance(value, pd.DataFrame):
            msg = f"The field '{field.name}' must be a pandas DataFrame, but got {type(value).__name__}."
            raise TypeError(msg)
        return value

    def tables(self) -> list[tuple[str, pd.core.frame.PandasDataFrame]]:
        tables = [(self._path(IntensityTableType.INTENSITY), self.intensity), \
                (self._path(IntensityTableType.METADATA), self.metadata)]
        if self.runtime_metadata is not None:
            tables.append((self._path(IntensityTableType.RUNTIME_METADATA), self.runtime_metadata))
        return tables

    def dump(self) -> dict:
        if self._dump_cache is None:
            self._dump_cache =  {
                    "name": self.name,
                    "type": self.dataset_type,
                    "run_id": self.run_id,
                    "tables": [
                        {
                            "id": str(uuid.uuid4()),
                            "name": "Protein_Intensity",
                            "path": self._path(IntensityTableType.INTENSITY),

                        },{
                            "id": str(uuid.uuid4()),
                            "name": "Protein_Metadata",
                            "path": self._path(IntensityTableType.METADATA),
                        },
                    ],
            }
            if self.runtime_metadata is not None:
                self._dump_cache["tables"].append({
                    "id": str(uuid.uuid4()),
                    "name": "Protein_RuntimeMetadata",
                    "path": self._path(IntensityTableType.RUNTIME_METADATA),
                })
        return self._dump_cache

    def _path(self, table_type: IntensityTableType) -> str:
        return f"job_runs/{self.run_id}/{table_type.value}.parquet"


class RFuncArgs(BaseModel):
    data_frames: list[pd.core.frame.PandasDataFrame]
    r_args: list[str]

    class Config:
        arbitrary_types_allowed = True  # Allow pandas DataFrame type
