from __future__ import annotations
import abc
import re
import uuid
from enum import Enum
from typing import TYPE_CHECKING
from typing import Literal
import pandas as pd
from md_form.field_utils.field_helpers import select_field
from md_form.field_utils.rules_builder import is_required
from md_form.field_utils.when import When
from pydantic import BaseModel
from pydantic import PrivateAttr
from pydantic import model_validator

if TYPE_CHECKING:
    from md_dataset.file_manager import FileManager


class DatasetType(Enum):
    INTENSITY = "INTENSITY"
    DOSE_RESPONSE = "DOSE_RESPONSE"
    PAIRWISE = "PAIRWISE"
    ANOVA = "ANOVA"
    ENRICHMENT = "ENRICHMENT"

class InputParams(BaseModel):
  pass

class EntityInputParams(InputParams):
  entity_type: Literal[
      "Peptide",
      "Protein",
  ] = select_field(
      name="Entity Type",
      description="Entity type of the intensity dataset",
      default="Protein",
      rules=[is_required()],
      when=When.not_equals("input_datasets", None),
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

    def populate_tables(self, file_manager: FileManager) -> InputDataset:
        tables = [
                InputDatasetTable(**table.dict(exclude={"data", "bucket", "key"}), \
                        data = file_manager.load_parquet_to_df( \
                            bucket = table.bucket, key = table.key)) \
                for table in self.tables]
        self.tables = tables

class IntensityEntity(str, Enum):
    PROTEIN = "Protein"
    PEPTIDE = "Peptide"

class IntensityTableType(Enum):
    INTENSITY = "intensity"
    METADATA = "metadata"
    RUNTIME_METADATA = "runtime_metadata"
    PTM_SITES = "ptm_sites"

class IntensityTable(BaseModel):
    type: IntensityTableType
    data: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def table_name(cls, intensity_type: IntensityTableType, \
            entity_type: IntensityEntity = IntensityEntity.PROTEIN) -> str:
        return f"{entity_type.value}_{to_pascal(intensity_type.value)}"

class IntensityInputDataset(InputDataset):
    type: DatasetType = DatasetType.INTENSITY

    def table(self, table_type: IntensityTableType, \
            entity_type: IntensityEntity = IntensityEntity.PROTEIN) -> InputDatasetTable:
        return next(filter(lambda table: table.name == IntensityTable.table_name(table_type, entity_type), \
                self.tables), None)

class Dataset(BaseModel, abc.ABC):
    run_id: uuid.UUID
    dataset_type: DatasetType

    @abc.abstractmethod
    def tables(self) -> list:
        pass

    @abc.abstractmethod
    def dump(self) -> dict:
        pass

class IntensityData(BaseModel):
    entity: IntensityEntity
    tables: list[IntensityTable]

    class Config:
        arbitrary_types_allowed = True

class IntensityDataset(Dataset):
    intensity_tables: list[IntensityData]
    _dump_cache: dict = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    @model_validator(mode="before")
    def validate_intensity_tables(cls, values: dict) -> dict:
        intensity_tables = values.get("intensity_tables")
        if intensity_tables is None:
            msg = "IntensityData must have field 'intensity_tables' set, it must be set and cannot be None."
            raise ValueError(msg)

        if not isinstance(intensity_tables, list):
            msg = f"The field 'intensity_tables' must be a list, but got {type(intensity_tables).__name__}."
            raise TypeError(msg)

        required_table_types = {IntensityTableType.INTENSITY, IntensityTableType.METADATA}

        for i, datum in enumerate(intensity_tables):
            if not isinstance(datum, IntensityData):
                msg = f"Each item in 'intensity_tables' must be an IntensityData object, but got \
                        {type(datum).__name__} at index {i}."
                raise TypeError(msg)

            table_types = {table.type for table in datum.tables}
            missing_types = required_table_types - table_types

            if missing_types:
                for missing_type in sorted(missing_types, key=lambda x: x.value):
                    msg = f"The field '{missing_type.value}' must be set and cannot be None."
                    raise ValueError(msg)

            for table in datum.tables:
                if not isinstance(table.data, pd.DataFrame):
                    msg = f"Table data must be a pandas DataFrame, but got {type(table.data).__name__} \
                            for table type {table.type.value} at index {i}."
                    raise TypeError(msg)

        return values

    def tables(self) -> list[tuple[str, pd.DataFrame]]:
        result = []
        for datum in self.intensity_tables:
            result.extend((self._path(datum.entity, table.type), table.data) for table in datum.tables)
        return result

    def dump(self) -> dict:
        if self._dump_cache is None:
            result_tables = []
            for datum in self.intensity_tables:
                result_tables.extend({
                    "id": str(uuid.uuid4()),
                    "name": "PTM_sites" if table.type == IntensityTableType.PTM_SITES
                            else f"{datum.entity.value}_{to_pascal(table.type.value)}",
                    "path": self._path(datum.entity, table.type),
                } for table in datum.tables)

            self._dump_cache = {
                "type": self.dataset_type,
                "run_id": self.run_id,
                "tables": result_tables,
            }
        return self._dump_cache

    def _path(self, entity: IntensityEntity, data_type: IntensityTableType) -> str:
        if data_type == IntensityTableType.PTM_SITES:
            return f"job_runs/{self.run_id}/PTM_sites.parquet"
        return f"job_runs/{self.run_id}/{entity.value}_{to_pascal(data_type.value)}.parquet"

def to_pascal(s: str) -> str:
    if not s:
        return s
    parts = re.split(r"[_\W]+", s.strip())
    return "".join(p[:1].upper() + p[1:].lower() for p in parts if p)

class EnrichmentTableType(Enum):
    RESULTS = "results"
    RUNTIME_METADATA = "runtime_metadata"
    DATABASE_METADATA = "database_metadata"


class EnrichmentDataset(Dataset):
    """A enrichment dataset.

    Attributes:
    ----------
    results :  PandasDataFrame
        The dataframe containing the enrichment results
    runtime_metadata : PandasDataFrame
        Information about the dataset at runtime
    database_metadata : PandasDataFrame
        Information about the database used for the enrichment analysis
    """
    results: pd.DataFrame
    runtime_metadata: pd.DataFrame = None
    database_metadata: pd.DataFrame = None
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

        database_metadata = values.get("database_metadata")
        if database_metadata is not None and not isinstance(database_metadata, pd.DataFrame):
            msg = f"The field 'database_metadata' must be a pandas DataFrame if provided, but \
                    got {type(database_metadata).__name__}."
            raise TypeError(msg)
        return values

    def tables(self) -> list[tuple[str, pd.DataFrame]]:
        tables = [(self._path(EnrichmentTableType.RESULTS), self.results)]
        if self.runtime_metadata is not None:
            tables.append((self._path(EnrichmentTableType.RUNTIME_METADATA), self.runtime_metadata))
        if self.database_metadata is not None:
            tables.append((self._path(EnrichmentTableType.DATABASE_METADATA), self.database_metadata))
        return tables

    def dump(self) -> dict:
        if self._dump_cache is None:
            self._dump_cache =  {
                    "type": self.dataset_type,
                    "run_id": self.run_id,
                    "tables": [
                        {
                            "id": str(uuid.uuid4()),
                            "name": "output_comparisons",
                            "path": self._path(EnrichmentTableType.RESULTS),
                        },
                        {
                            "id": str(uuid.uuid4()),
                            "name": "runtime_metadata",
                            "path": self._path(EnrichmentTableType.RUNTIME_METADATA),
                        },
                        {
                            "id": str(uuid.uuid4()),
                            "name": "database_metadata",
                            "path": self._path(EnrichmentTableType.DATABASE_METADATA),
                        },
                    ],
            }
        return self._dump_cache

    def _path(self, table_type: EnrichmentTableType) -> str:
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

    def dump(self) -> dict:
        if self._dump_cache is None:
            self._dump_cache =  {
                    "type": self.dataset_type,
                    "run_id": self.run_id,
                    "tables": [
                        {
                            "id": str(uuid.uuid4()),
                            "name": "output_comparisons",
                            "path": self._path(PairwiseTableType.RESULTS),
                        },
                        {
                            "id": str(uuid.uuid4()),
                            "name": "runtime_metadata",
                            "path": self._path(PairwiseTableType.RUNTIME_METADATA),
                        },
                    ],
            }
        return self._dump_cache

    def _path(self, table_type: PairwiseTableType) -> str:
        return f"job_runs/{self.run_id}/{table_type.value}.parquet"


class AnovaTableType(Enum):
    RESULTS = "results"
    RUNTIME_METADATA = "runtime_metadata"


class AnovaDataset(Dataset):
    """An ANOVA dataset.

    Attributes:
    -----------
    results : pd.DataFrame
        The dataframe containing the ANOVA analysis results.
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
        tables = [(self._path(AnovaTableType.RESULTS), self.results)]
        if self.runtime_metadata is not None:
            tables.append((self._path(AnovaTableType.RUNTIME_METADATA), self.runtime_metadata))
        return tables

    def dump(self) -> dict:
        if self._dump_cache is None:
            self._dump_cache = {
                "type": self.dataset_type,
                "run_id": self.run_id,
                "tables": [
                    {
                        "id": str(uuid.uuid4()),
                        "name": "anova_results",
                        "path": self._path(AnovaTableType.RESULTS),
                    },
                    {
                        "id": str(uuid.uuid4()),
                        "name": "runtime_metadata",
                        "path": self._path(AnovaTableType.RUNTIME_METADATA),
                    },
                ],
            }
        return self._dump_cache

    def _path(self, table_type: AnovaTableType) -> str:
        return f"job_runs/{self.run_id}/{table_type.value}.parquet"


class DoseResponseTableType(Enum):
    OUTPUT_CURVES = "output_curves"
    OUTPUT_VOLCANOES = "output_volcanoes"
    INPUT_DRC = "input_drc"
    RUNTIME_METADATA = "runtime_metadata"

class DoseResponseDataset(Dataset):
    """A dose-response dataset.

    Attributes:
    ----------
    output_curves :  PandasDataFrame
        The dataframe containing the dose-response results to create dose-response curves
    output_volcanoes : PandasDataFrame
        The dataframe containing the dose-response results to create dose-response volcano plots
    input_drc : PandasDataFrame
        The dataframe containing the dose-response input data
    runtime_metadata : PandasDataFrame
        Information about the dataset at runtime
    """
    output_curves: pd.DataFrame
    output_volcanoes: pd.DataFrame
    input_drc: pd.DataFrame
    runtime_metadata: pd.DataFrame = None
    _dump_cache: dict = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    @model_validator(mode="before")
    def validate_dataframes(cls, values: dict) -> dict:
        required_fields = ["output_curves", "output_volcanoes", "input_drc"]
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
        tables = [
            (self._path(DoseResponseTableType.OUTPUT_CURVES), self.output_curves),
            (self._path(DoseResponseTableType.OUTPUT_VOLCANOES), self.output_volcanoes),
            (self._path(DoseResponseTableType.INPUT_DRC), self.input_drc),
        ]
        if self.runtime_metadata is not None:
            tables.append((self._path(DoseResponseTableType.RUNTIME_METADATA), self.runtime_metadata))
        return tables

    def dump(self, entity_type: str | None = None) -> dict: # noqa: ARG002
        if self._dump_cache is None:
            self._dump_cache =  {
                    "type": self.dataset_type,
                    "run_id": self.run_id,
                    "tables": [
                        {
                            "id": str(uuid.uuid4()),
                            "name": "output_curves",
                            "path": self._path(DoseResponseTableType.OUTPUT_CURVES),
                        },
                    {
                            "id": str(uuid.uuid4()),
                            "name": "output_volcanoes",
                            "path": self._path(DoseResponseTableType.OUTPUT_VOLCANOES),
                        },
                        {
                            "id": str(uuid.uuid4()),
                            "name": "input_drc",
                            "path": self._path(DoseResponseTableType.INPUT_DRC),
                        },
                        {
                            "id": str(uuid.uuid4()),
                            "name": "runtime_metadata",
                            "path": self._path(DoseResponseTableType.RUNTIME_METADATA),
                        },
                    ],
            }
        return self._dump_cache

    def _path(self, table_type: DoseResponseTableType) -> str:
        return f"job_runs/{self.run_id}/{table_type.value}.parquet"

class LegacyIntensityDataset(Dataset):
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

    def dump(self) -> dict:
        if self._dump_cache is None:
            self._dump_cache =  {
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
