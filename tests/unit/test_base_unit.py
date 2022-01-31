from unittest.mock import call, Mock, MagicMock

from dags.base import (
    callable_download_s3_resources_task,
    callable_push_s3_collection_task,
    callable_push_s3_dataset_task,
)


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


def test_push_s3_dataset(kwargs, transformed_dir, issue_dir, dataset_dir, mocker):
    #  Setup
    mock_s3_client = MagicMock()

    # Call
    with mocker.patch(
        "dags.base._get_environment", return_value="production"
    ), mocker.patch(
        "airflow.models.Variable.get", return_value="iamacollections3bucket"
    ), mocker.patch(
        "dags.base._get_s3_client", return_value=mock_s3_client
    ):
        callable_push_s3_dataset_task(**kwargs)
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
    kwargs, collection_resources_dir, collection_metadata_dir, mocker
):
    #  Setup
    mock_s3_client = MagicMock()

    # Call
    with mocker.patch(
        "dags.base._get_environment", return_value="production"
    ), mocker.patch(
        "airflow.models.Variable.get", return_value="iamacollections3bucket"
    ), mocker.patch(
        "dags.base._get_s3_client", return_value=mock_s3_client
    ):
        callable_push_s3_collection_task(**kwargs)
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
