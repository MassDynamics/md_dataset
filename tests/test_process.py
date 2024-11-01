import uuid
import pytest
from md_dataset.models.types import JobRunParams
from md_dataset.process import md_process


@md_process
def run_process(id_: uuid.UUID, params: JobRunParams):
    return [1, id_, params]


@pytest.fixture
def id_() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def name() -> str:
    return "run123"


@pytest.fixture
def params() -> JobRunParams:
    return JobRunParams(names=["one"])


def test_run_process_returns_data(id_: uuid.UUID, name: str, params: JobRunParams):
    assert run_process(id_, name, params).data()[0].iloc[0] == 1


def test_run_process_sets_name(id_: uuid.UUID, name: str, params: JobRunParams):
    assert run_process(id_, name, params).data_sets[0].name == "run123"
