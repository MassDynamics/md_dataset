from typing import Callable
import pandas as pd
from md_dataset.models.types import DatasetParams
from md_dataset.models.types import FlowOutPut
from md_dataset.models.types import FlowOutPutDataSet
from md_dataset.models.types import FlowOutPutTable


def md_process(func: Callable) -> Callable:
    def wrapper(params: DatasetParams) -> FlowOutPut:
        results = func(params)

        return FlowOutPut(
            data_sets=[
                FlowOutPutDataSet(
                    name=params.name,
                    type=params.type,
                    tables=[
                        FlowOutPutTable(name="Protein_Intensity", data=pd.DataFrame(results)),
                    ],
                ),
            ],
        )

    return wrapper
