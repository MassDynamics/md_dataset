from typing import Callable
from uuid import UUID
import pandas as pd
from md_dataset.models.types import DataSetType
from md_dataset.models.types import FlowOutPut
from md_dataset.models.types import FlowOutPutDataSet
from md_dataset.models.types import FlowOutPutTable
from md_dataset.models.types import JobRunParams


def md_process(func: Callable) -> Callable:
    def wrapper(uuid: UUID, name: str, params: JobRunParams) -> FlowOutPut:
        results = func(uuid, params)

        return FlowOutPut(
            data_sets=[
                FlowOutPutDataSet(
                    name=name,
                    type=DataSetType.INTENSITY,
                    tables=[
                        FlowOutPutTable(name="Protein_Intensity", data=pd.DataFrame(results)),
                    ],
                ),
            ],
        )

    return wrapper
