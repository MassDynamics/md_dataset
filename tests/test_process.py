import uuid
import pytest
from md_dataset.models.types import DatasetParams
from md_dataset.models.types import DatasetType
from md_dataset.process import md_process


@md_process
def run_process(params: DatasetParams):
    return [1, params]


@pytest.fixture
def id_() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def name() -> str:
    return "run123"


@pytest.fixture
def params() -> DatasetParams:
    return DatasetParams(name="one", source_location="baz/qux", type=DatasetType.INTENSITY)


def test_run_process_returns_data(params: DatasetParams):
    assert run_process(params).data()[0].iloc[0] == 1


def test_run_process_sets_name(params: DatasetParams):
    assert run_process(params).data_sets[0].name == "one"
