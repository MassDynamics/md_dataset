import logging
import pandas as pd
from md_data_set.models.types import DataSetType
from md_data_set.models.types import FlowOutPut
from md_data_set.models.types import FlowOutPutDataSet
from md_data_set.models.types import FlowOutPutTable


def md_process(func):
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(__name__)
        logger.info("pre")
        results = func(*args, **kwargs)

        return FlowOutPut(
                data_sets=[
                    FlowOutPutDataSet(
                        name="md",
                        type=DataSetType.INTENSITY,
                        tables=[
                            FlowOutPutTable(name="Protein_Intensity",
                                            data=pd.DataFrame(results)),
                            ],
                        ),
                    ],
                )

    return wrapper

