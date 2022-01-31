import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from shutil import rmtree

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import boto3
from cloudpathlib import CloudPath
from git import Repo
from humps import pascalize
import requests

from digital_land.api import DigitalLandApi
from digital_land.specification import specification_path


def _get_environment():
    return os.environ.get("ENVIRONMENT", "development")


def _get_api_instance(kwargs):
    pipeline_name = kwargs["dag"]._dag_id

    collection_repository_path = _get_collection_repository_path(kwargs)
    pipeline_dir = Path(collection_repository_path).joinpath("pipeline")
    assert pipeline_dir.exists()

    logging.info(
        f"Instantiating DigitalLandApi for pipeline {pipeline_name} using pipeline "
        f"directory: {pipeline_dir} and specification_directory {specification_path}"
    )
    return DigitalLandApi(
        debug=False,
        pipeline_name=pipeline_name,
        pipeline_dir=str(pipeline_dir),
        specification_dir=str(specification_path),
    )


def _get_collection_repository_path(kwargs):
    return Path(kwargs["ti"].xcom_pull(key="collection_repository_path"))


def _get_pipeline_name(kwargs):
    return kwargs["dag"]._dag_id


def _get_repo_name(kwargs):
    return f"{_get_pipeline_name(kwargs)}-collection"


_get_resources_name = _get_repo_name


def _get_temporary_directory():
    return Path("/tmp")


def _get_s3_client():
    return boto3.client("s3")


def _upload_directory_to_s3(directory: Path, destination: str):
    files = directory.iterdir()
    _upload_files_to_s3(files, destination)


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
    pipeline_name = _get_pipeline_name(kwargs)
    run_id = kwargs["run_id"]
    directory = Path("/tmp").joinpath(f"{pipeline_name}_{run_id}")
    directory.mkdir(exist_ok=True)
    path = directory.joinpath("organisation.csv")
    organisation_csv_url = Variable.get("organisation_csv_url")
    response = requests.get(organisation_csv_url)
    response.raise_for_status()
    with open(path, "w+") as file:
        file.write(response.text)
    return path


def callable_clone_task(**kwargs):
    # TODO parameteriize git ref to use, and writer automated tests
    pipeline_name = _get_pipeline_name(kwargs)
    run_id = kwargs["run_id"]
    repo_name = _get_repo_name(kwargs)
    repo_path = (
        _get_temporary_directory()
        .joinpath(f"{pipeline_name}_{run_id}")
        .joinpath(repo_name)
    )

    repo_path.mkdir(parents=True)
    Repo.clone_from(f"https://github.com/digital-land/{repo_name}", to_path=repo_path)
    kwargs["ti"].xcom_push("collection_repository_path", str(repo_path))


def callable_collect_task(**kwargs):
    collection_repository_path = _get_collection_repository_path(kwargs)
    api = _get_api_instance(kwargs)

    endpoint_path = Path(collection_repository_path).joinpath("collection/endpoint.csv")
    collection_dir = Path(collection_repository_path).joinpath("collection")

    logging.info(
        f"Calling collect_cmd with endpoint_path {endpoint_path} and collection_dir {collection_dir}"
    )

    api.collect_cmd(endpoint_path=endpoint_path, collection_dir=collection_dir)
    kwargs["ti"].xcom_push("api_instance", api.to_json())


def callable_download_s3_resources_task(**kwargs):
    s3_resource_name = _get_resources_name(kwargs)
    collection_s3_bucket = Variable.get("collection_s3_bucket")
    collection_repository_path = _get_collection_repository_path(kwargs)

    s3_resource_path = (
        f"s3://{collection_s3_bucket}/{s3_resource_name}/collection/resource/"
    )
    destination_dir = (
        Path(collection_repository_path).joinpath("collection").joinpath("resource")
    )
    destination_dir.mkdir(parents=True)
    cp = CloudPath(s3_resource_path)
    cp.download_to(destination_dir)
    logging.info(
        f"Copied resources from {s3_resource_path} to {destination_dir} . Got: {list(destination_dir.iterdir())}"
    )


def callable_collection_task(**kwargs):
    api = _get_api_instance(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)
    collection_dir = Path(collection_repository_path).joinpath("collection")
    logging.info(
        f"Calling pipeline_collection_save_csv_cmd with collection_dir {collection_dir}"
    )
    api.pipeline_collection_save_csv_cmd(collection_dir=collection_dir)


def callable_commit_task(**kwargs):
    environment = _get_environment()
    if environment != "production":
        raise AirflowSkipException(
            f"Doing nothing as $ENVIRONMENT is {environment} and not 'production'"
        )
    collection_repository_path = _get_collection_repository_path(kwargs)
    repo = Repo(collection_repository_path)
    repo.git.add(update=False)
    repo.index.commit(f"Data {datetime.now().isoformat()}")
    repo.remotes["origin"].push()


def callable_dataset_task(**kwargs):
    api = _get_api_instance(kwargs)
    pipeline_name = _get_pipeline_name(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)

    collection_dir = Path(collection_repository_path).joinpath("collection")
    resource_dir = collection_dir.joinpath("resource")
    resource_list = resource_dir.iterdir()
    organisation_csv_path = _get_organisation_csv(kwargs)
    issue_dir = collection_repository_path.joinpath("issue").joinpath(pipeline_name)
    issue_dir.mkdir(parents=True)
    collection_repository_path.joinpath("transformed").joinpath(pipeline_name).mkdir(
        exist_ok=True, parents=True
    )
    collection_repository_path.joinpath("harmonised").joinpath(pipeline_name).mkdir(
        exist_ok=True, parents=True
    )

    for resource_file in resource_list:
        # Most digital_land.API() commands expect strings not pathlib.Path
        pipeline_cmd_args = {
            "input_path": str(resource_file),
            "output_path": str(
                collection_repository_path.joinpath("transformed")
                .joinpath(pipeline_name)
                .joinpath(resource_file.name)
            ),
            "collection_dir": collection_dir,
            "null_path": None,
            "issue_dir": issue_dir,
            "organisation_path": organisation_csv_path,
            "save_harmonised": True,
        }
        log_string = (
            f"digital-land --pipeline-name {pipeline_name} pipeline "
            f"--issue-dir {pipeline_cmd_args['issue_dir']} "
            f" {pipeline_cmd_args['input_path']} {pipeline_cmd_args['output_path']} "
            f"--null-path {pipeline_cmd_args['null_path']} "
            f"--organisation-path {pipeline_cmd_args['organisation_path']} "
        )
        if pipeline_cmd_args["save_harmonised"]:
            log_string += " --save-harmonised"

        logging.info(log_string)

        api.pipeline_cmd(**pipeline_cmd_args)


def callable_build_dataset_task(**kwargs):
    api = _get_api_instance(kwargs)
    pipeline_name = _get_pipeline_name(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)

    collection_dir = Path(collection_repository_path).joinpath("collection")
    resource_list = Path(collection_dir).joinpath("resource").iterdir()
    potential_input_paths = [
        collection_repository_path.joinpath("transformed")
        .joinpath(pipeline_name)
        .joinpath(resource_file.name)
        for resource_file in resource_list
    ]
    actual_input_paths = list(filter(lambda x: x.exists(), potential_input_paths))
    if potential_input_paths != actual_input_paths:
        logging.warning(
            "The following expected output files were not generated by `digital-land pipeline`: {}".format(
                set(potential_input_paths).difference(actual_input_paths)
            )
        )

    dataset_path = collection_repository_path.joinpath("dataset")
    dataset_path.mkdir()
    sqlite_artifact_path = dataset_path.joinpath(
        f"{pipeline_name}.sqlite3",
    )
    unified_collection_csv_path = dataset_path.joinpath(
        f"{pipeline_name}.csv",
    )
    # Most digital_land.API() commands expect strings not pathlib.Path
    actual_input_paths_str = list(map(str, actual_input_paths))
    sqlite_artifact_path_str = str(sqlite_artifact_path)
    unified_collection_csv_path_str = str(unified_collection_csv_path)

    logging.info(
        f"digital-land --pipeline-name {pipeline_name} load-entries "
        f" {actual_input_paths_str} {sqlite_artifact_path_str}"
    )

    api.load_entries_cmd(actual_input_paths_str, sqlite_artifact_path_str)

    logging.info(
        f"digital-land --pipeline-name {pipeline_name} build-dataset "
        f" {sqlite_artifact_path_str} {unified_collection_csv_path_str}"
    )
    api.build_dataset_cmd(sqlite_artifact_path_str, unified_collection_csv_path_str)


def callable_push_s3_collection_task(**kwargs):
    environment = _get_environment()
    if environment != "production":
        raise AirflowSkipException(
            f"Doing nothing as $ENVIRONMENT is {environment} and not 'production'"
        )
    pipeline_name = _get_pipeline_name(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)

    _upload_directory_to_s3(
        directory=collection_repository_path.joinpath("collection").joinpath(
            "resource"
        ),
        destination=f"{pipeline_name}/collection/resource",
    )

    _upload_files_to_s3(
        files=[
            collection_repository_path.joinpath("collection").joinpath("endpoint.csv"),
            collection_repository_path.joinpath("collection").joinpath("log.csv"),
            collection_repository_path.joinpath("collection").joinpath("resource.csv"),
            collection_repository_path.joinpath("collection").joinpath("source.csv"),
        ],
        destination=f"{pipeline_name}/collection",
    )


def callable_push_s3_dataset_task(**kwargs):
    environment = _get_environment()
    if environment != "production":
        raise AirflowSkipException(
            f"Doing nothing as $ENVIRONMENT is {environment} and not 'production'"
        )
    pipeline_name = _get_pipeline_name(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)

    _upload_directory_to_s3(
        directory=collection_repository_path.joinpath("transformed").joinpath(
            pipeline_name,
        ),
        destination=f"{pipeline_name}/transformed",
    )

    _upload_directory_to_s3(
        directory=collection_repository_path.joinpath("issue").joinpath(
            pipeline_name,
        ),
        destination=f"{pipeline_name}/issue",
    )

    _upload_directory_to_s3(
        directory=collection_repository_path.joinpath("dataset"),
        destination=f"{pipeline_name}/dataset",
    )


def callable_working_directory_cleanup_task(**kwargs):
    collection_repository_path = _get_collection_repository_path(kwargs)
    rmtree(collection_repository_path)


def kebab_to_pascal_case(kebab_case_str):
    return pascalize(kebab_case_str.replace("-", "_"))


# TODO autopopulate these by finding repos ending in `-collection` within `digital-land`
pipelines = [
    "ancient-woodland",
    "article-4-direction",
    "brownfield-land",
    "brownfield-site",
    "conservation-area",
    "dataset",
    "development-plan-document",
    "development-policy-area",
    "development-policy",
    "listed-building",
    "tree-preservation-order",
]
for pipeline_name in pipelines:
    with DAG(
        pipeline_name, schedule_interval=timedelta(days=1), start_date=datetime.now()
    ) as InstantiatedDag:

        clone = PythonOperator(task_id="clone", python_callable=callable_clone_task)
        download_s3_resources = PythonOperator(
            task_id="download_s3_resources",
            python_callable=callable_download_s3_resources_task,
        )
        collect = PythonOperator(
            task_id="collect", python_callable=callable_collect_task
        )
        collection = PythonOperator(
            task_id="collection", python_callable=callable_collection_task
        )
        commit_collect = PythonOperator(
            task_id="commit_collect", python_callable=callable_commit_task
        )
        commit_collection = PythonOperator(
            task_id="commit_collection", python_callable=callable_commit_task
        )
        commit_harmonised = PythonOperator(
            task_id="commit_harmonised", python_callable=callable_commit_task
        )
        push_s3_collection = PythonOperator(
            task_id="push_s3_collection",
            python_callable=callable_push_s3_collection_task,
        )
        dataset = PythonOperator(
            task_id="dataset", python_callable=callable_dataset_task
        )
        build_dataset = PythonOperator(
            task_id="build_dataset", python_callable=callable_build_dataset_task
        )
        push_s3_dataset = PythonOperator(
            task_id="push_s3_dataset",
            python_callable=callable_push_s3_dataset_task,
        )
        working_directory_cleanup = PythonOperator(
            task_id="working_directory_cleaanup",
            python_callable=callable_working_directory_cleanup_task,
        )

        clone >> download_s3_resources
        download_s3_resources >> collect
        collect >> commit_collect
        collect >> collection
        collection >> commit_collection
        commit_collect >> commit_collection
        collection >> push_s3_collection
        collection >> dataset
        dataset >> build_dataset
        build_dataset >> commit_harmonised
        build_dataset >> push_s3_dataset
        commit_harmonised >> working_directory_cleanup
        push_s3_dataset >> working_directory_cleanup
        push_s3_collection >> working_directory_cleanup
        commit_collection >> working_directory_cleanup

        # Airflow likes to be able to find its DAG's as module scoped variables
        globals()[f"{kebab_to_pascal_case(pipeline_name)}Dag"] = InstantiatedDag
