import pytest
from md_data_set.process import md_process

@md_process
def run_init(num: int):
    return 1 + num

def test_run_init():
    assert run_init(1) == 2
