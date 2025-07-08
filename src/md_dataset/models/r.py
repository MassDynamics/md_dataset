from __future__ import annotations
import pandas as pd
from pydantic import BaseModel
from pydantic import model_validator


class RFuncArgs(BaseModel):
    data_frames: list[pd.DataFrame]
    r_args: list[str] = []

    class Config:
        arbitrary_types_allowed = True

    @model_validator(mode="before")
    def validate_data_frames(cls, values: dict) -> dict:
        data_frames = values.get("data_frames")
        if not isinstance(data_frames, list):
            msg = "data_frames must be a list."
            raise TypeError(msg)
        if not all(isinstance(df, pd.DataFrame) for df in data_frames):
            msg = "All items in data_frames must be pandas DataFrame objects."
            raise TypeError(msg)
        return values
