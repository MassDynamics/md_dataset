from __future__ import annotations
from enum import Enum
from typing import TypeVar
import pandas as pd
from pydantic import BaseModel
from pydantic import conlist

pd.core.frame.PandasDataFrame = TypeVar("pd.core.frame.DataFrame")


class DatasetParams(BaseModel):
    name: str
    source_location: str
    type: DatasetType


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

    def data(self) -> list:
        return self.data_sets[0].tables[0].data
