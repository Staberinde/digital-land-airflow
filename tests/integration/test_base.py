from datetime import datetime
from pathlib import Path
from shutil import copytree
from unittest.mock import Mock

import pytest

from dags.base import callable_collect_task


@pytest.fixture
def collection_metadata_dir(tmpdir):
    collection_dir = tmpdir.join('collection')
    copytree(Path(__file__).parent.parent.joinpath('data/collection'), collection_dir, dirs_exist_ok=True)
    return collection_dir


@pytest.fixture
def endpoint_requests_mock(requests_mock, collection_metadata_dir):
    with open(collection_metadata_dir.join('endpoint.csv')) as f:
        endpoint_contents = DictReader(f)
        return [requests_mock.get(row['endpoint-url']) for row in endpoint_contents]


def test_collect(endpoint_requests_mock, tmpdir, collection_metadata_dir):
    tmpdir.mkdir('pipeline')

    kwargs = {
        "ti": Mock(**{"xcom_pull.return_value": tmpdir}),
        "dag": Mock(**{"_dag_id": "listed-building"}),
    }
    response = callable_collect_task(**kwargs)
