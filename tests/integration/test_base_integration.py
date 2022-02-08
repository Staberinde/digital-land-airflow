from csv import DictReader
from datetime import date
from filecmp import cmpfiles
from json import load

from dags.base import (
    callable_build_dataset_task,
    callable_collect_task,
    callable_collection_task,
    callable_dataset_task,
)

TODAY = date.today()


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

    harmonised_dir = tmp_path.joinpath("harmonised")

    issue_dir = tmp_path.joinpath("issue")

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
    assert len(list(transformed_dir.glob("**/*"))) == len(
        list(test_expected_results_dir.joinpath("transformed").glob("**/*"))
    ), list(transformed_dir.glob("**/*"))

    cmpfiles(
        harmonised_dir,
        test_expected_results_dir.joinpath("harmonised"),
        test_expected_results_dir.joinpath("harmonised").iterdir(),
        shallow=False,
    )
    assert len(list(harmonised_dir.glob("**/*"))) == len(
        list(test_expected_results_dir.joinpath("harmonised").glob("**/*"))
    ), list(harmonised_dir.glob("**/*"))

    cmpfiles(
        issue_dir,
        test_expected_results_dir.joinpath("issue"),
        test_expected_results_dir.joinpath("issue").iterdir(),
        shallow=False,
    )
    assert len(list(issue_dir.glob("**/*"))) == len(
        list(test_expected_results_dir.joinpath("issue").glob("**/*"))
    ), list(issue_dir.glob("**/*"))


def test_dataset_specified_resources(
    collection_metadata_dir,
    collection_resources_dir,
    collection_resources_file,
    data_dir,
    expected_results_dir,
    kwargs_specified_resources,
    mocker,
    requests_mock,
    tmp_path,
):
    test_expected_results_dir = expected_results_dir.joinpath("test_dataset")
    # Setup
    tmp_path.joinpath("pipeline").mkdir()

    transformed_dir = tmp_path.joinpath("transformed")

    harmonised_dir = tmp_path.joinpath("harmonised")

    issue_dir = tmp_path.joinpath("issue")

    # Call
    with mocker.patch(
        "dags.base._get_organisation_csv",
        return_value=data_dir.joinpath("organisation.csv"),
    ):
        callable_dataset_task(**kwargs_specified_resources)

    # Assert
    cmpfiles(
        transformed_dir,
        test_expected_results_dir.joinpath("transformed"),
        [
            test_expected_results_dir.joinpath("transformed").joinpath(resource)
            for resource in kwargs_specified_resources["params"]["resource_hashes"]
        ],
        shallow=False,
    )
    assert len(list(transformed_dir.glob("**/*"))) == len(
        list(test_expected_results_dir.joinpath("transformed").glob("**/*"))
    ), list(transformed_dir.glob("**/*"))

    cmpfiles(
        harmonised_dir,
        test_expected_results_dir.joinpath("harmonised"),
        [
            test_expected_results_dir.joinpath("harmonised").joinpath(resource)
            for resource in kwargs_specified_resources["params"]["resource_hashes"]
        ],
        shallow=False,
    )
    assert len(list(harmonised_dir.glob("**/*"))) == len(
        list(test_expected_results_dir.joinpath("harmonised").glob("**/*"))
    ), list(harmonised_dir.glob("**/*"))

    cmpfiles(
        issue_dir,
        test_expected_results_dir.joinpath("issue"),
        [
            test_expected_results_dir.joinpath("issue").joinpath(resource)
            for resource in kwargs_specified_resources["params"]["resource_hashes"]
            if test_expected_results_dir.joinpath("issue").joinpath(resource).exists()
        ],
        shallow=False,
    )
    assert len(list(issue_dir.glob("**/*"))) == len(
        list(test_expected_results_dir.joinpath("issue").glob("**/*"))
    ), list(issue_dir.glob("**/*"))


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
    assert len(list(tmp_path.joinpath("dataset").glob("**/*"))) == len(
        list(test_expected_results_dir.joinpath("dataset").glob("**/*"))
    )


def test_build_dataset_specified_resources(
    collection_resources_dir,
    expected_results_dir,
    transformed_dir,
    kwargs_specified_resources,
    tmp_path,
):
    # Setup
    tmp_path.joinpath("pipeline").mkdir()
    test_expected_results_dir = expected_results_dir.joinpath("test_build_dataset")

    # Call
    callable_build_dataset_task(**kwargs_specified_resources)

    # Assert
    cmpfiles(
        tmp_path.joinpath("dataset"),
        test_expected_results_dir.joinpath("dataset"),
        [
            test_expected_results_dir.joinpath("dataset").joinpath(resource)
            for resource in kwargs_specified_resources["params"]["resource_hashes"]
        ],
        shallow=False,
    )
    assert len(list(tmp_path.joinpath("dataset").glob("**/*"))) == len(
        list(test_expected_results_dir.joinpath("dataset").glob("**/*"))
    )
