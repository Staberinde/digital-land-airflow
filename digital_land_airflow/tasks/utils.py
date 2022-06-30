import logging
import os
from pathlib import Path
import requests

from airflow.models import Variable
import boto3

from digital_land.api import DigitalLandApi
from digital_land.specification import specification_path


class ResourceNotFound(Exception):
    """Raised if explicitly specified resource cannot be found in resources/"""

    pass


def _get_environment():
    return os.environ.get("ENVIRONMENT", "development")


def _get_api_instance(kwargs, dataset_name=None):
    if not dataset_name:
        dataset_name = kwargs["dag"]._dag_id

    collection_repository_path = _get_collection_repository_path(kwargs)
    pipeline_dir = collection_repository_path.joinpath("pipeline")
    # This is a simple assertion to verify the collection we are operating on is a modern one generated from collection-template
    # as these are the only ones we can work with
    # This may be able to be safely removed in the future
    assert pipeline_dir.exists()

    logging.info(
        f"Instantiating DigitalLandApi for pipeline {dataset_name} using pipeline "
        f"directory: {pipeline_dir} and specification_directory {specification_path}"
    )
    return DigitalLandApi(
        debug=False,
        dataset=dataset_name,
        pipeline_dir=str(pipeline_dir),
        specification_dir=str(specification_path),
    )


def _get_collection_repository_path(kwargs):
    return Path(kwargs["ti"].xcom_pull(key="collection_repository_path"))


def _get_collection_name(kwargs):
    return kwargs["dag"]._dag_id


def _get_repo_name(kwargs):
    return f"{_get_collection_name(kwargs)}-collection"


_get_resources_name = _get_repo_name


def _get_temporary_directory():
    return Path(Variable.get("temp_directory_root"))


def _get_run_temporary_directory(kwargs):
    collection_name = _get_collection_name(kwargs)
    run_id = kwargs["run_id"]
    return _get_temporary_directory().joinpath(f"{collection_name}_{run_id}")


def _get_s3_client():
    return boto3.client("s3")


def _filter_specified_resources(kwargs, resource_list):
    specified_resources = kwargs.get("params", {}).get("specified_resources", [])
    if specified_resources:
        resource_list = list(filter(lambda x: x in specified_resources, resource_list))
        if len(resource_list) < len(specified_resources):
            missing_resources = set(specified_resources).difference(
                set(resource.name for resource in resource_list)
            )
            raise ResourceNotFound(
                f"Could not find following specified resources: {missing_resources}"
            )
    return resource_list


def _get_pipeline_resource_mapping(kwargs):
    api = _get_api_instance(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)
    collection_dir = collection_repository_path.joinpath("collection")
    pipeline_resource_mapping = api.pipeline_resource_mapping_for_collection(
        collection_dir
    )
    return {
        pipeline: _filter_specified_resources(kwargs, resources)
        for pipeline, resources in pipeline_resource_mapping.items()
    }


def _get_resource_pipeline_mapping(kwargs):
    pipeline_resource_mapping = _get_pipeline_resource_mapping(kwargs)
    resource_pipeline_mapping = {}
    for pipeline, resource_set in pipeline_resource_mapping.items():
        for resource in resource_set:
            resource_pipeline_mapping.setdefault(resource, []).append(pipeline)
    return resource_pipeline_mapping


def _upload_directory_to_s3(directory: Path, destination: str):
    if directory.exists():
        files = directory.iterdir()
        _upload_files_to_s3(files, destination)
    else:
        logging.warning(
            f"Directory {directory} not present in working directory!. Skipping upload to S3"
        )


def _upload_files_to_s3(files, destination):
    s3_client = _get_s3_client()
    collection_s3_bucket = Variable.get("collection_s3_bucket")
    for file_to_upload in files:
        s3_client.upload_file(
            str(file_to_upload),
            collection_s3_bucket,
            f"{destination}/{file_to_upload.name}",
        )


def _get_organisation_csv(kwargs):
    if (
        "ORGANISATION_CSV_PATH" in os.environ
        and Path(os.environ["ORGANISATION_CSV_PATH"]).exists()
    ):
        return Path(os.environ["ORGANISATION_CSV_PATH"])
    directory = _get_run_temporary_directory(kwargs)
    directory.mkdir(exist_ok=True)
    path = directory.joinpath("organisation.csv")
    organisation_csv_url = Variable.get("organisation_csv_url")
    logging.info(
        f"Fetching organisation.csv from {organisation_csv_url}, saving to ${path}"
    )
    response = requests.get(organisation_csv_url)
    response.raise_for_status()
    with open(path, "w+") as file:
        file.write(response.text)
    return path


def is_run_harmonised_stage(collection_name):
    return "brownfield-land" in collection_name
