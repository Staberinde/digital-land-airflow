from csv import DictReader
from datetime import date
from filecmp import cmpfiles
from json import load
from pathlib import Path
from shutil import copy, copytree
from unittest.mock import Mock, MagicMock

import pytest

from dags.base import (
    callable_build_dataset_task,
    callable_collect_task,
    callable_collection_task,
    callable_dataset_task,
    callable_download_s3_resources_task,
)


TODAY = date.today()


class DigitalLandAirflowTestSetupException(Exception):
    """This is to make it really obvious where to look when it throws"""

    pass


@pytest.fixture
def data_dir():
    return Path(__file__).parent.parent.joinpath("data")


@pytest.fixture
def collection_resources_file(data_dir, tmp_path):
    collection_dir = tmp_path.joinpath("collection")
    collection_dir.mkdir(exist_ok=True)
    resource_file = collection_dir.joinpath("resource.csv")

    if resource_file.exists():
        print(
            f"{resource_file.name} exists from previous fixture, replacing with populatd version"
        )
    resource_file.touch()
    copy(
        data_dir.joinpath("collection").joinpath("resources").joinpath("resource.csv"),
        resource_file,
    )
    return resource_file


@pytest.fixture
def collection_resources_dir(data_dir, tmp_path):
    resources_dir = tmp_path.joinpath("collection").joinpath("resource")
    copytree(
        data_dir.joinpath("collection").joinpath("resources").joinpath("resource"),
        resources_dir,
    )
    return resources_dir


@pytest.fixture
def transformed_dir(data_dir, tmp_path):
    transformed_dir = tmp_path.joinpath("transformed")
    copytree(
        data_dir.joinpath("transformed"),
        transformed_dir,
    )
    return transformed_dir


@pytest.fixture
def collection_metadata_dir(data_dir, tmp_path):
    collection_dir = tmp_path.joinpath("collection")
    copytree(
        data_dir.joinpath("collection").joinpath("csv"),
        collection_dir,
        dirs_exist_ok=True,
    )
    return collection_dir


@pytest.fixture
def collection_payload_dir(data_dir, tmp_path):
    log_dir = (
        tmp_path.joinpath("collection").joinpath("log").joinpath(TODAY.isoformat())
    )
    copytree(
        data_dir.joinpath("collection").joinpath("log"),
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
def expected_results_dir(data_dir):
    return data_dir.joinpath("expected_results")


@pytest.fixture
def kwargs(tmp_path):
    def _xcom_pull_return(key):
        if key == "collection_repository_path":
            return tmp_path
        else:
            raise DigitalLandAirflowTestSetupException(
                f"I don't yet know what to do with xcom_pull arg {key}"
            )

    return {
        "ti": Mock(**{"xcom_pull.side_effect": _xcom_pull_return}),
        "dag": Mock(**{"_dag_id": "listed-building"}),
        # This should increment with each test execution
        "run_id": tmp_path.parent.name.split("-")[-1],
    }


def test_download_s3_resources(kwargs, mocker, tmp_path):
    mock_s3_request = MagicMock(
        return_value=(
            Mock(status_code=200),
            {
                "ContentLength": 7,
                "Body": MagicMock(**{"read.side_effect": [b"29183723", b""]}),
            },
        ),
    )
    with mocker.patch(
        "airflow.models.Variable.get", return_value="iamacollections3bucket"
    ), mocker.patch(
        "botocore.client.BaseClient._make_request", side_effect=mock_s3_request
    ):
        callable_download_s3_resources_task(**kwargs)
        assert all(
            [
                request[1][1]["url_path"]
                == "/iamacollections3bucket/listed-building-collection/collection/resource/"
                for request in mock_s3_request.mock_calls
            ]
        )


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
    data_dir,
    expected_results_dir,
    kwargs,
    mocker,
    requests_mock,
    tmp_path,
):
    test_expected_results_dir = expected_results_dir.joinpath("test_dataset")
    # Setup
    tmp_path.joinpath("pipeline").mkdir()

    transformed_dir = tmp_path.joinpath("transformed")
    transformed_dir.joinpath("brownfield-land").mkdir(parents=True)
    transformed_dir.joinpath("listed-building").mkdir()

    harmonised_dir = tmp_path.joinpath("harmonised")
    harmonised_dir.joinpath("brownfield-land").mkdir(parents=True)
    harmonised_dir.joinpath("listed-building").mkdir()

    issue_dir = tmp_path.joinpath("issue")
    issue_dir.joinpath("brownfield-land").mkdir(parents=True)

    # Call
    with mocker.patch(
        "dags.base._get_organisation_csv",
        return_value=data_dir.joinpath("organisation.csv"),
    ):
        callable_dataset_task(**kwargs)

    # Assert
    cmpfiles(
        transformed_dir,
        test_expected_results_dir.joinpath("transformed"),
        test_expected_results_dir.joinpath("transformed").iterdir(),
        shallow=False,
    )
    cmpfiles(
        harmonised_dir,
        test_expected_results_dir.joinpath("harmonised"),
        test_expected_results_dir.joinpath("harmonised").iterdir(),
        shallow=False,
    )
    cmpfiles(
        issue_dir,
        test_expected_results_dir.joinpath("issue"),
        test_expected_results_dir.joinpath("issue").iterdir(),
        shallow=False,
    )


def test_build_dataset(
    collection_resources_dir, expected_results_dir, transformed_dir, kwargs, tmp_path
):
    # Setup
    tmp_path.joinpath("pipeline").mkdir()
    test_expected_results_dir = expected_results_dir.joinpath("test_build_dataset")

    # Call
    callable_build_dataset_task(**kwargs)

    # Assert
    cmpfiles(
        tmp_path.joinpath("dataset"),
        test_expected_results_dir.joinpath("dataset"),
        test_expected_results_dir.joinpath("dataset").iterdir(),
        shallow=False,
    )
