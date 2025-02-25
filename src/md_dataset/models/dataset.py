from __future__ import annotations
import abc
import uuid
from enum import Enum
from typing import TYPE_CHECKING
from typing import Literal
import pandas as pd
from pydantic import BaseModel
from pydantic import Field
from pydantic import PrivateAttr
from pydantic import model_validator

if TYPE_CHECKING:
    from md_dataset.file_manager import FileManager


class DatasetType(Enum):
    INTENSITY = "INTENSITY"
    DOSE_RESPONSE = "DOSE_RESPONSE"
    PAIRWISE = "PAIRWISE"

class InputParams(BaseModel):
  """The name of the dataset.

  Attributes:
  ----------
  dataset_name : str
    The name of the Dataset to create, if None we use legacy names from MD dataset service
  """
  dataset_name: str = None

class ConverterInputParams(InputParams):
  entity_type: Literal[
      "peptide",
      "protein",
  ] = Field(
      title="Entity Type",
      description="Entity type of the intensity dataset",
      default="protein",
  )

class InputDatasetTable(BaseModel):
    name: str
    bucket: str = None
    key: str = None
    data: pd.DataFrame = None

    class Config:
        arbitrary_types_allowed = True

class InputDataset(BaseModel):
    id: uuid.UUID
    name: str
    job_run_params: dict = {}
    type: DatasetType
    tables: list[InputDatasetTable]

    def table_by_name(self, name: str) -> InputDatasetTable:
        return next(filter(lambda table: table.name == name, self.tables), None)

    def table_data_by_name(self, name: str) -> pd.DataFrame:
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

    def table(self, table_type: IntensityTableType) -> InputDatasetTable:
        return next(filter(lambda table: table.name == IntensityTable.table_name(table_type), \
                self.tables), None)

class DoseResponseTableType(Enum):
    OUTPUT_CURVES = "output_curves"
    OUTPUT_VOLCANOES = "output_volcanoes"
    INPUT_DRC = "input_drc"
    RUNTIME_METADATA = "runtime_metadata"

class DoseResponseInputDataset(InputDataset):
    type: DatasetType = DatasetType.DOSE_RESPONSE

    def table(self, table_type: DoseResponseTableType) -> InputDatasetTable:
        return next(filter(lambda table: table.name == table_type.value, self.tables), None)

class Dataset(BaseModel, abc.ABC):
    run_id: uuid.UUID
    name: str
    dataset_type: DatasetType

    @abc.abstractmethod
    def tables(self) -> list:
        pass

    @abc.abstractmethod
    def dump(self, entity_type: str | None = None) -> dict:
        pass

    @classmethod
    def from_run(cls, run_id: uuid.UUID, name: str, dataset_type: DatasetType, tables: list) -> Dataset:
        if dataset_type == DatasetType.INTENSITY:
            return IntensityDataset(run_id=run_id, name=name, dataset_type=dataset_type, \
                    **tables)
        if dataset_type == DatasetType.PAIRWISE:
            return PairwiseDataset(run_id=run_id, name=name, dataset_type=dataset_type, \
                    **tables)
        return None # TODO raise

class IntensityDataset(Dataset):
    """An intentisy dataset.

    Attributes:
    ----------
    intensity :  PandasDataFrame
        The dataframe containing intensity values
    metadata : PandasDataFrame
        Information about the dataset
    runtime_metadata : PandasDataFrame
        Information about the dataset at runtime
    """
    intensity: pd.DataFrame
    metadata: pd.DataFrame
    runtime_metadata: pd.DataFrame = None
    _dump_cache: dict = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    @model_validator(mode="before")
    def validate_dataframes(cls, values: dict) -> dict:
        required_fields = ["intensity", "metadata"]
        for field_name in required_fields:
            value = values.get(field_name)
            if value is None:
                msg = f"The field '{field_name}' must be set and cannot be None."
                raise ValueError(msg)
            if not isinstance(value, pd.DataFrame):
                msg = f"The field '{field_name}' must be a pandas DataFrame, but got {type(value).__name__}."
                raise TypeError(msg)

        runtime_metadata = values.get("runtime_metadata")
        if runtime_metadata is not None and not isinstance(runtime_metadata, pd.DataFrame):
            msg = f"The field 'runtime_metadata' must be a pandas DataFrame if provided, but \
                    got {type(runtime_metadata).__name__}."
            raise TypeError(msg)
        return values

    def tables(self) -> list[tuple[str, pd.DataFrame]]:
        tables = [(self._path(IntensityTableType.INTENSITY), self.intensity), \
                (self._path(IntensityTableType.METADATA), self.metadata)]
        if self.runtime_metadata is not None:
            tables.append((self._path(IntensityTableType.RUNTIME_METADATA), self.runtime_metadata))
        return tables

    def dump(self, entity_type: str | None = "protein") -> dict:
        if self._dump_cache is None:
            self._dump_cache =  {
                    "name": self.name,
                    "type": self.dataset_type,
                    "run_id": self.run_id,
                    "tables": [
                        {
                            "id": str(uuid.uuid4()),
                            "name": f"{entity_type.title()}_Intensity",
                            "path": self._path(IntensityTableType.INTENSITY),

                        },{
                            "id": str(uuid.uuid4()),
                            "name": f"{entity_type.title()}_Metadata",
                            "path": self._path(IntensityTableType.METADATA),
                        },
                    ],
            }
            if self.runtime_metadata is not None:
                self._dump_cache["tables"].append({
                    "id": str(uuid.uuid4()),
                    "name": f"{entity_type.title()}_RuntimeMetadata",
                    "path": self._path(IntensityTableType.RUNTIME_METADATA),
                })
        return self._dump_cache

    def _path(self, table_type: IntensityTableType) -> str:
        return f"job_runs/{self.run_id}/{table_type.value}.parquet"

class PairwiseTableType(Enum):
    RESULTS = "results"
    RUNTIME_METADATA = "runtime_metadata"


class PairwiseDataset(Dataset):
    """A Pairwise dataset.

    Attributes:
    ----------
    results :  PandasDataFrame
        The dataframe containing the pairwise results
    runtime_metadata : PandasDataFrame
        Information about the dataset at runtime
    """
    results: pd.DataFrame
    runtime_metadata: pd.DataFrame = None
    _dump_cache: dict = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    @model_validator(mode="before")
    def validate_dataframes(cls, values: dict) -> dict:
        required_fields = ["results"]
        for field_name in required_fields:
            value = values.get(field_name)
            if value is None:
                msg = f"The field '{field_name}' must be set and cannot be None."
                raise ValueError(msg)
            if not isinstance(value, pd.DataFrame):
                msg = f"The field '{field_name}' must be a pandas DataFrame, but got {type(value).__name__}."
                raise TypeError(msg)

        runtime_metadata = values.get("runtime_metadata")
        if runtime_metadata is not None and not isinstance(runtime_metadata, pd.DataFrame):
            msg = f"The field 'runtime_metadata' must be a pandas DataFrame if provided, but \
                    got {type(runtime_metadata).__name__}."
            raise TypeError(msg)
        return values

    def tables(self) -> list[tuple[str, pd.DataFrame]]:
        tables = [(self._path(PairwiseTableType.RESULTS), self.results)]
        if self.runtime_metadata is not None:
            tables.append((self._path(PairwiseTableType.RUNTIME_METADATA), self.runtime_metadata))
        return tables

    def dump(self, entity_type: str | None = None) -> dict:  # noqa: ARG002
        if self._dump_cache is None:
            self._dump_cache =  {
                    "name": self.name,
                    "type": self.dataset_type,
                    "run_id": self.run_id,
                    "tables": [
                        {
                            "id": str(uuid.uuid4()),
                            "name": "output_comparisons",
                            "path": self._path(PairwiseTableType.RESULTS),
                        },
                    ],
            }
            if self.runtime_metadata is not None:
                self._dump_cache["tables"].append({
                    "id": str(uuid.uuid4()),
                    "name": "runtime_metadata",
                    "path": self._path(PairwiseTableType.RUNTIME_METADATA),
                })
        return self._dump_cache

    def _path(self, table_type: PairwiseTableType) -> str:
        return f"job_runs/{self.run_id}/{table_type.value}.parquet"
