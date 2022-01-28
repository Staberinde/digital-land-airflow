from csv import DictReader
from datetime import date
from json import load
from pathlib import Path
from shutil import copy, copytree
from unittest.mock import Mock

import pytest

from dags.base import (
    callable_collect_task,
    callable_collection_task,
    callable_dataset_task,
)


TODAY = date.today()


@pytest.fixture
def collection_resources_file(tmp_path):
    collection_dir = tmp_path.joinpath("collection")
    collection_dir.mkdir(exist_ok=True)
    resource_file = collection_dir.joinpath("resource.csv")

    if resource_file.exists():
        print(f"{resource_file.name} exists from previous fixture, replacing with populatd version")
    resource_file.touch()
    copy(
        Path(__file__).parent.parent.joinpath("data/collection/resources/resource.csv"),
        resource_file,
    )
    return resource_file


@pytest.fixture
def collection_resources_dir(tmp_path):
    resources_dir = tmp_path.joinpath("collection").joinpath("resource")
    copytree(
        Path(__file__).parent.parent.joinpath("data/collection/resources/resource"),
        resources_dir,
    )
    return resources_dir


@pytest.fixture
def collection_metadata_dir(tmp_path):
    collection_dir = tmp_path.joinpath("collection")
    copytree(
        Path(__file__).parent.parent.joinpath("data/collection/csv"),
        collection_dir,
        dirs_exist_ok=True,
    )
    return collection_dir


@pytest.fixture
def collection_payload_dir(tmp_path):
    log_dir = (
        tmp_path.joinpath("collection").joinpath("log").joinpath(TODAY.isoformat())
    )
    copytree(
        Path(__file__).parent.parent.joinpath("data/collection/log"),
        log_dir,
        dirs_exist_ok=True,
    )
    return log_dir


@pytest.fixture
def endpoint_requests_mock(requests_mock, collection_metadata_dir):
    with open(collection_metadata_dir.joinpath("endpoint.csv")) as f:
        endpoint_contents = DictReader(f)
        return [
            requests_mock.get(
                row["endpoint-url"], json={"iamaresponsefrom": row["endpoint-url"]}
            )
            for row in endpoint_contents
            # Implicit test of this logic in the code as requests mock will throw
            # requests_mock.exceptions.NoMockAddress if un-mocked URL requested
            if not row["end-date"] or date.fromisoformat(row["end-date"]) >= TODAY
        ]


@pytest.fixture
def kwargs(tmp_path):
    return {
        "ti": Mock(**{"xcom_pull.return_value": tmp_path}),
        "dag": Mock(**{"_dag_id": "listed-building"}),
        # This should increment with each test execution
        "run_id": tmp_path.parent.name.split("-")[-1],
    }


def test_collect(collection_metadata_dir, endpoint_requests_mock, kwargs, tmp_path):
    # Setup
    tmp_path.joinpath("pipeline").mkdir()

    # Call
    callable_collect_task(**kwargs)

    # Assert
    log_dir = collection_metadata_dir.joinpath("log").joinpath(TODAY.isoformat())
    all_urls = {endpoint._url for endpoint in endpoint_requests_mock}
    all_logs = [load(f.open()) for f in log_dir.iterdir()]
    assert {log["endpoint-url"] for log in all_logs} == all_urls
    for mock in endpoint_requests_mock:
        assert mock.called_once
        assert mock._url in [log["endpoint-url"] for log in all_logs]


def test_collection(collection_metadata_dir, collection_payload_dir, kwargs, tmp_path):
    # Setup
    tmp_path.joinpath("pipeline").mkdir()

    # Call
    callable_collection_task(**kwargs)

    # Assert
    with open(collection_payload_dir.parent.parent.joinpath("log.csv")) as log_file:
        log_csv = DictReader(log_file)
        logs = list(log_csv)
        assert {log["endpoint"] for log in logs} == set(
            path.name[: -len("".join(path.suffixes))]
            for path in collection_payload_dir.iterdir()
        )

        with open(
            collection_payload_dir.parent.parent.joinpath("resource.csv")
        ) as resource_file:
            resource_csv = DictReader(resource_file)
            assert {log["resource"] for log in logs} == {
                resource["resource"] for resource in resource_csv
            }


def test_dataset(
    collection_metadata_dir,
    collection_resources_dir,
    collection_resources_file,
    requests_mock,
    mocker,
    kwargs,
    tmp_path,
):
    # Setup
    tmp_path.joinpath("pipeline").mkdir()
    tmp_path.joinpath("transformed").joinpath("brownfield-land").mkdir(parents=True)
    tmp_path.joinpath("transformed").joinpath("listed-building").mkdir()
    tmp_path.joinpath("harmonised").joinpath("brownfield-land").mkdir(parents=True)
    tmp_path.joinpath("harmonised").joinpath("listed-building").mkdir()
    tmp_path.joinpath("issue").joinpath("brownfield-land").mkdir(parents=True)

    # Call
    with mocker.patch(
        "dags.base._get_organisation_csv",
        return_value=Path(__file__).parent.parent.joinpath("data/organisation.csv"),
    ):
        callable_dataset_task(**kwargs)

    # Assert
