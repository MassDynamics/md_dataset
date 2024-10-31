from enum import Enum
from typing import List
from typing import TypeVar
from pydantic import BaseModel
from pydantic import conlist

PandasDataFrame = TypeVar("pandas.core.frame.DataFrame")


class DataSetType(Enum):
    INTENSITY = "INTENSITY"


class FlowOutPutTable(BaseModel):
    name: str
    data: PandasDataFrame


class FlowOutPutDataSet(BaseModel):
    name: str
    tables: List[FlowOutPutTable]
    type: DataSetType


class FlowOutPut(BaseModel):
    data_sets: conlist(FlowOutPutDataSet, min_length=1, max_length=1)

    def data(self):
        return self.data_sets[0].tables[0].data
