import logging
from typing import Callable
import pandas as pd
from md_dataset.models.types import DataSetType
from md_dataset.models.types import FlowOutPut
from md_dataset.models.types import FlowOutPutDataSet
from md_dataset.models.types import FlowOutPutTable


def md_process(func: Callable) -> Callable:
    def wrapper(num: int) -> FlowOutPut:
        logger = logging.getLogger(__name__)
        logger.info("pre")
        results = func(num)

        return FlowOutPut(
            data_sets=[
                FlowOutPutDataSet(
                    name="md",
                    type=DataSetType.INTENSITY,
                    tables=[
                        FlowOutPutTable(name="Protein_Intensity", data=pd.DataFrame(results)),
                    ],
                ),
            ],
        )

    return wrapper
