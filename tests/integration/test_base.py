from csv import DictReader
from datetime import datetime, date
from json import load
from pathlib import Path
from shutil import copytree
from unittest.mock import Mock

import pytest

from dags.base import callable_collect_task


@pytest.fixture
def collection_metadata_dir(tmpdir):
    collection_dir = tmpdir.join("collection")
    copytree(
        Path(__file__).parent.parent.joinpath("data/collection"),
        collection_dir,
        dirs_exist_ok=True,
    )
    return collection_dir


@pytest.fixture
def endpoint_requests_mock(requests_mock, collection_metadata_dir):
    with open(collection_metadata_dir.join("endpoint.csv")) as f:
        endpoint_contents = DictReader(f)
        return [
            requests_mock.get(row["endpoint-url"])
            for row in endpoint_contents
            # Implicit test of this logic in the code as requests mock will throw
            # requests_mock.exceptions.NoMockAddress if un-mocked URL requested
            if not row["end-date"]
            or date.fromisoformat(row["end-date"]) >= date.today()
        ]


def test_collect(endpoint_requests_mock, tmpdir, collection_metadata_dir):
    # Setup
    tmpdir.mkdir("pipeline")
    kwargs = {
        "ti": Mock(**{"xcom_pull.return_value": tmpdir}),
        "dag": Mock(**{"_dag_id": "listed-building"}),
    }

    # Call
    response = callable_collect_task(**kwargs)

    # Assert
    log_dir = collection_metadata_dir.join("log").join(date.today().isoformat())
    all_urls = {endpoint._url for endpoint in endpoint_requests_mock}
    all_logs = [load(f.open()) for f in log_dir.listdir()]
    assert {log["endpoint-url"] for log in all_logs} == all_urls
    for mock in endpoint_requests_mock:
        assert mock.called_once
        assert mock._url in [log["endpoint-url"] for log in all_logs]
