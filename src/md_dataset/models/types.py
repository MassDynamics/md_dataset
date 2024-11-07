from __future__ import annotations
from enum import Enum
from typing import TYPE_CHECKING
from pydantic import BaseModel
from pydantic import conlist

if TYPE_CHECKING:
    import pandas as pd


class DatasetParams(BaseModel):
    name: str
    tables: dict
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

    def data(self, i: int) -> list:
        return self.data_sets[0].tables[i].data

