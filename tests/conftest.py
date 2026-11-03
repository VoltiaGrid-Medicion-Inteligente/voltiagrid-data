"""Shared fixtures. One Spark session for the whole test run: starting Spark takes seconds."""

import shutil

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    if shutil.which("java") is None:
        pytest.skip("Java is not installed")
    from jobs.spark import get_spark

    session = get_spark("voltiagrid-tests", master="local[2]", driver_memory="1g", shuffle_partitions=2, ui=False)
    yield session
    session.stop()
