from csv import DictReader
from datetime import date
from pathlib import Path
from shutil import copy, copytree
from unittest.mock import Mock

import pytest

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
            f"{resource_file.name} exists from previous fixture, replacing with populated version"
        )
    resource_file.touch()
    copy(
        data_dir.joinpath("collection").joinpath("resources").joinpath("resource.csv"),
        resource_file,
    )
    return resource_file


@pytest.fixture
def collection_logs_file(data_dir, tmp_path):
    collection_dir = tmp_path.joinpath("collection")
    collection_dir.mkdir(exist_ok=True)
    log_file = collection_dir.joinpath("log.csv")

    if log_file.exists():
        print(
            f"{log_file.name} exists from previous fixture, replacing with populated version"
        )
    log_file.touch()
    copy(
        data_dir.joinpath("collection").joinpath("resources").joinpath("log.csv"),
        log_file,
    )
    return log_file


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
def issue_dir(data_dir, tmp_path):
    issue_dir = tmp_path.joinpath("issue")
    copytree(
        data_dir.joinpath("issue"),
        issue_dir,
    )
    return issue_dir


@pytest.fixture
def dataset_dir(data_dir, tmp_path):
    dataset_dir = tmp_path.joinpath("dataset")
    copytree(
        data_dir.joinpath("dataset"),
        dataset_dir,
    )
    return dataset_dir


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
def organisation_csv_url(requests_mock, data_dir):
    fake_organisation_csv_url = "https://iamanorganisationcsvurl"

    organisation_csv_fixture = data_dir.joinpath("organisation.csv")
    requests_mock.get(
        fake_organisation_csv_url, text=organisation_csv_fixture.open().readline()
    )
    return fake_organisation_csv_url


@pytest.fixture(autouse=True)
def airflow_variable(mocker, organisation_csv_url):
    def _variable_return(key):
        if key == "organisation_csv_url":
            return organisation_csv_url
        elif key == "temp_directory_root":
            return "/tmp"
        else:
            raise DigitalLandAirflowTestSetupException(
                f"I don't yet know what to do with Variable {key}"
            )

    mocker.patch("airflow.models.Variable.get", side_effect=_variable_return)


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


@pytest.fixture
def kwargs_specified_resources(kwargs):
    kwargs.update(
        {
            "params": {
                "resource_hashes": [
                    "72337bced0ee7e6f7ec339822e3ec1a55dbd02729a1df3747308ebdd49905868",
                    "efbdafb929921097a6e002188e281047bb4d512d40a8f88ade26cbb44118f0e3",
                ]
            }
        }
    )
    return kwargs


@pytest.fixture
def column_field_dir(data_dir, tmp_path):
    column_field_dir = tmp_path.joinpath("var").joinpath("column-field")
    copytree(
        data_dir.joinpath("var").joinpath("column-field"),
        column_field_dir,
        dirs_exist_ok=True,
    )
    return column_field_dir


@pytest.fixture
def dataset_resource_dir(data_dir, tmp_path):
    column_field_dir = tmp_path.joinpath("var").joinpath("dataset-resource")
    copytree(
        data_dir.joinpath("var").joinpath("dataset-resource"),
        column_field_dir,
        dirs_exist_ok=True,
    )
    return column_field_dir



@pytest.fixture
def pipeline_dir(data_dir, tmp_path):
    pipeline_dir = tmp_path.joinpath("pipeline")
    copytree(
        data_dir.joinpath("pipeline"),
        pipeline_dir,
        dirs_exist_ok=True,
    )
    return pipeline_dir
