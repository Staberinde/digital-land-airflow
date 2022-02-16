from unittest.mock import call, Mock, MagicMock

from dags.base import (
    callable_clone_task,
    callable_commit_task,
    callable_download_s3_resources_task,
    callable_push_s3_task,
    _get_organisation_csv,
)


def test_clone(kwargs, mocker, tmp_path):
    expected_path = tmp_path.joinpath(f"listed-building_{kwargs['run_id']}").joinpath(
        "listed-building-collection"
    )
    mocker.patch("dags.base._get_temporary_directory", return_value=tmp_path)
    mock_git_repo_clone_from = mocker.patch("git.Repo.clone_from")
    assert not expected_path.exists()
    callable_clone_task(**kwargs)
    assert expected_path.exists()
    mock_git_repo_clone_from.assert_called_once_with(
        "https://github.com/digital-land/listed-building-collection",
        to_path=expected_path,
    )


def test_download_s3_resources(kwargs, collection_resources_dir, mocker, tmp_path):
    #  Setup
    mocker.patch("airflow.models.Variable.get", return_value="iamacollections3bucket")
    mock_s3_request = mocker.patch(
        "botocore.client.BaseClient._make_request",
    )
    mock_s3_request.configure_mock(
        return_value=(
            Mock(status_code=200),
            {
                "ContentLength": 7,
                "Body": MagicMock(**{"read.side_effect": [b"29183723", b""]}),
            },
        ),
    )
    # Call
    callable_download_s3_resources_task(**kwargs)
    # Assert
    assert all(
        [
            request[1][1]["url_path"]
            == "/iamacollections3bucket/listed-building-collection/collection/resource/"
            for request in mock_s3_request.mock_calls
        ]
    )


def test_commit(kwargs, mocker, tmp_path):
    # Setup
    tmp_path.joinpath("foo").touch()
    kwargs["paths_to_commit"] = ["foo"]
    mocker.patch("dags.base._get_environment", return_value="production")
    push_mock = MagicMock()
    mock_repo = mocker.patch("dags.base.Repo")
    mock_repo.configure_mock(
        **{
            "return_value.remotes.__getitem__.return_value.urls": ["iamaurl"],
            "return_value.remotes.__getitem__.return_value.push": push_mock,
        }
    )
    # Call
    callable_commit_task(**kwargs)
    # Assert
    mock_repo.assert_called_once_with(tmp_path)
    mock_repo.return_value.git.add.assert_called_once_with("foo")
    mock_repo.return_value.index.commit.assert_called_once()
    push_mock.assert_called_once()


def test_push_s3_dataset(
    collection_metadata_dir,
    collection_payload_dir,  # This is now a dependency of api.pipeline_resource_mapping_for_collection_
    collection_resources_dir,  # This is now a dependency of api.pipeline_resource_mapping_for_collection_
    kwargs,
    transformed_dir,
    issue_dir,
    dataset_dir,
    mocker,
    tmp_path,
):
    #  Setup
    tmp_path.joinpath("pipeline").mkdir()

    kwargs["directories_to_push"] = [
        ("transformed/{dataset_name}", "{dataset_name}/transformed"),
        ("issue/{dataset_name}", "{dataset_name}/issue"),
        ("dataset", "{dataset_name}/dataset"),
    ]
    kwargs["files_to_push"] = []
    mock_s3_client = MagicMock()
    mocker.patch("dags.base._get_environment", return_value="production")
    mocker.patch("airflow.models.Variable.get", return_value="iamacollections3bucket")
    mocker.patch("dags.base._get_s3_client", return_value=mock_s3_client)
    # Call
    callable_push_s3_task(**kwargs)
    for collection_dir in transformed_dir.iterdir():
        mock_s3_client.assert_has_calls(
            [
                call.upload_file(
                    str(path),
                    "iamacollections3bucket",
                    f"{collection_dir.name}/transformed/{path.name}",
                )
                for path in collection_dir.iterdir()
            ]
        )
    for collection_dir in issue_dir.iterdir():
        mock_s3_client.assert_has_calls(
            [
                call.upload_file(
                    str(path),
                    "iamacollections3bucket",
                    f"{collection_dir.name}/issue/{path.name}",
                )
                for path in collection_dir.iterdir()
            ]
        )
    mock_s3_client.assert_has_calls(
        [
            call.upload_file(
                str(path),
                "iamacollections3bucket",
                f"listed-building/dataset/{path.name}",
            )
            for path in dataset_dir.iterdir()
        ]
    )


def test_push_s3_collection(
    kwargs,
    collection_resources_dir,
    collection_metadata_dir,
    collection_payload_dir,
    mocker,
):
    #  Setup
    kwargs["directories_to_push"] = [
        ("collection/resource", "{dataset_name}/collection/resource"),
    ]
    kwargs["files_to_push"] = [
        (
            [
                "collection/endpoint.csv",
                "collection/log.csv",
                "collection/resource.csv",
                "collection/source.csv",
            ],
            "{dataset_name}/collection",
        ),
    ]
    mock_s3_client = MagicMock()
    mocker.patch("dags.base._get_environment", return_value="production")
    mocker.patch("airflow.models.Variable.get", return_value="iamacollections3bucket")
    mocker.patch("dags.base._get_s3_client", return_value=mock_s3_client)
    # Call
    callable_push_s3_task(**kwargs)
    # Assert
    mock_s3_client.assert_has_calls(
        [
            call.upload_file(
                str(path),
                "iamacollections3bucket",
                f"listed-building/collection/resource/{path.name}",
            )
            for path in collection_resources_dir.iterdir()
        ]
    )
    mock_s3_client.assert_has_calls(
        [
            call.upload_file(
                str(path),
                "iamacollections3bucket",
                f"listed-building/collection/{path.name}",
            )
            for path in sorted(collection_metadata_dir.iterdir())
            if path.name not in ["resource", "collection.csv"]
        ]
    )


def test_get_organisation_csv(kwargs, data_dir, mocker, requests_mock, tmp_path):
    # Setup
    organisation_csv_fixture = data_dir.joinpath("organisation.csv")
    fake_organisation_csv_url = "https://iamanorganisationcsvurl"
    mocker.patch("dags.base._get_run_temporary_directory", return_value=tmp_path)
    mocker.patch("airflow.models.Variable.get", return_value=fake_organisation_csv_url)
    requests_mock.get(
        fake_organisation_csv_url, text=organisation_csv_fixture.open().readline()
    )
    expected_path = tmp_path.joinpath("organisation.csv")

    # Call
    response = _get_organisation_csv(kwargs)
    # Assert

    assert response == expected_path
    with expected_path.open() as f:
        actual_content = f.readline()
    with organisation_csv_fixture.open() as f:
        expected_content = f.readline()
    assert actual_content == expected_content
