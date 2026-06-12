import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    # Boot a single ephemeral Prefect API server for the whole test session
    # (per xdist worker). Since prefect 3.3+ the harness starts a uvicorn
    # subprocess per invocation, so booting one per test caused intermittent
    # failures.
    with prefect_test_harness(server_startup_timeout=60):
        yield
