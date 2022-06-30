import logging

from airflow.exceptions import AirflowSkipException

from digital_land_airflow.tasks.utils import (
    _get_collection_repository_path,
    _get_api_instance,
)


def callable_collect_task(**kwargs):
    if kwargs.get("params", {}).get("specified_resources", []):
        raise AirflowSkipException(
            "Doing nothing as params['specified_resources'] is set"
        )
    collection_repository_path = _get_collection_repository_path(kwargs)
    api = _get_api_instance(kwargs)

    endpoint_path = collection_repository_path.joinpath("collection/endpoint.csv")
    collection_dir = collection_repository_path.joinpath("collection")

    logging.info(
        f"Calling collect_cmd with endpoint_path {endpoint_path} and collection_dir {collection_dir}"
    )

    api.collect_cmd(endpoint_path=endpoint_path, collection_dir=collection_dir)
    kwargs["ti"].xcom_push("api_instance", api.to_json())


def callable_collection_task(**kwargs):
    if kwargs.get("params", {}).get("specified_resources", []):
        raise AirflowSkipException(
            "Doing nothing as params['specified_resources'] is set"
        )
    api = _get_api_instance(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)
    collection_dir = collection_repository_path.joinpath("collection")
    logging.info(
        f"Calling pipeline_collection_save_csv_cmd with collection_dir {collection_dir}"
    )
    api.collection_save_csv_cmd(collection_dir=collection_dir)
