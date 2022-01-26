import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
import boto3
from cloudpathlib import CloudPath
from git import Repo
from humps import pascalize

from digital_land.api import DigitalLandApi
from digital_land.specification import specification_path


ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")


def _get_api_instance(kwargs):
    pipeline_name = kwargs['dag']._dag_id

    collection_repository_path = _get_collection_repository_path(kwargs)
    pipeline_dir = os.path.join(collection_repository_path, 'pipeline')
    assert os.path.exists(pipeline_dir)

    logging.info(
        f"Instantiating DigitalLandApi for pipeline {pipeline_name} using pipeline "
        f"directory: {pipeline_dir} and specification_directory {specification_path}"
    )
    return DigitalLandApi(
        debug=False,
        pipeline_name=pipeline_name,
        pipeline_dir=pipeline_dir,
        specification_dir=specification_path
    )


def _get_collection_repository_path(kwargs):
    return kwargs['ti'].xcom_pull(key="collection_repository_path")


def _upload_directory_to_s3(directory, destination):
    files = os.listdir(directory)
    _upload_files_to_s3(files, directory, destination)


def _upload_files_to_s3(files, directory, destination):
    s3 = boto3.resource("s3")
    collection_s3_bucket = Variable.get("collection_s3_bucket")
    for file_to_upload in files:
        s3.meta.client.upload_file(
            os.path.join(directory, file_to_upload),
            collection_s3_bucket,
            f"{destination}/{file_to_upload}",
        )


@task(
    task_id="clone",
)
def callable_clone_task(**kwargs):
    dag = kwargs['dag']
    run_id = kwargs['run_id']
    ti = kwargs['ti']
    pipeline_name = dag._dag_id
    repo_name = f"{pipeline_name}-collection"
    # TODO add onsuccess branch to delete this dir
    repo_path = os.path.join("/tmp", f"{pipeline_name}_{run_id}", repo_name)

    os.makedirs(repo_path)
    repo = Repo.clone_from(f"https://github.com/digital-land/{repo_name}", to_path=repo_path)
    ti.xcom_push("collection_repository_path", repo_path)


@task(
    task_id="collect",
)
def callable_collect_task(**kwargs):
    collection_repository_path = _get_collection_repository_path(kwargs)
    api = _get_api_instance(kwargs)

    endpoint_path = os.path.join(collection_repository_path, "collection/endpoint.csv")
    collection_dir = os.path.join(collection_repository_path, 'collection')

    logging.info(
        f"Calling collect_cmd with endpoint_path {endpoint_path} and collection_dir {collection_dir}"
    )

    api.collect_cmd(
        endpoint_path=endpoint_path,
        collection_dir=collection_dir
    )
    kwargs["ti"].xcom_push("api_instance", api.to_json())


@task(
    task_id="download_s3_resources"
)
def callable_download_s3_resources_task(**kwargs):
    dag = kwargs['dag']
    pipeline_name = dag._dag_id
    collection_s3_bucket = Variable.get("collection_s3_bucket")
    collection_repository_path = _get_collection_repository_path(kwargs)

    s3_resource_path = f"s3://{collection_s3_bucket}/{pipeline_name}/collection/resource/"
    destination_dir = os.path.join(collection_repository_path, "collection", "resource")
    cp = CloudPath(s3_resource_path)
    cp.download_to(
        os.path.join(collection_repository_path, "collection", "resource")
    )
    logging.info(
        f"Copied resources from {s3_resource_path} to {destination_dir} . Got: {os.listdir(destination_dir)}"
    )


@task(
    task_id="collection",
)
def callable_collection_task(**kwargs):
    api = _get_api_instance(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)
    collection_dir = os.path.join(collection_repository_path, 'collection')
    logging.info(
        f"Calling pipeline_collection_save_csv_cmd with collection_dir {collection_dir}"
    )
    api.pipeline_collection_save_csv_cmd(
        collection_dir=collection_dir
    )


@task(
    task_id="commit",
)
def callable_commit_task(**kwargs):
    if ENVIRONMENT != "production":
        raise AirflowSkipException(f"Doing nothing as $ENVIRONMENT is {ENVIRONMENT} and not 'production'")
    collection_repository_path = _get_collection_repository_path(kwargs)
    repo = Repo(collection_repository_path)
    repo.git.add(update=False)
    repo.index.commit(f"Data {datetime.now().isoformat()}")
    repo.remotes["origin"].push()


@task(
    task_id="dataset",
)
def callable_dataset_task(**kwargs):
    api = _get_api_instance(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)

    collection_dir = os.path.join(collection_repository_path, 'collection')
    resource_list = os.listdir(
        os.path.join(collection_dir, 'resource')
    )

    for resource_file in resource_list:
        pipeline_cmd_args = dict(
            input_path=os.path.join(
                collection_dir,
                "resource",
                resource_file
            ),
            output_path=os.path.join(
                collection_repository_path,
                "transformed",
                pipeline_name,
                resource_file
            ),
            collection_dir=collection_dir,
            null_path=None,
            issue_dir=f"issue/{pipeline_name}",
            organisation_path="/var/cache/organisation.csv",
            save_harmonised=False,
        )
        log_string = (
            f"digital-land --pipeline-name {pipeline_name} pipeline "
            f"--issue-dir {pipeline_cmd_args['issue_dir']} "
            f" {pipeline_cmd_args['input_path']} {pipeline_cmd_args['output_path']} "
            f"--null-path {pipeline_cmd_args['null_path']} "
            f"--organisation-path {pipeline_cmd_args['null_path']} "
        )
        if pipeline_cmd_args['save_harmonised']:
            log_string += " --save-harmonised"

        logging.info(log_string)

        api.pipeline_cmd(
            **pipeline_cmd_args
        )


@task(
    task_id="build_dataset",
)
def callable_build_dataset_task(**kwargs):
    api = _get_api_instance(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)

    collection_dir = os.path.join(collection_repository_path, 'collection')
    resource_list = os.listdir(
        os.path.join(collection_dir, 'resource')
    )
    potential_input_paths = [
        os.path.join(
            collection_repository_path,
            "transformed",
            pipeline_name,
            resource_file
        ) for resource_file in resource_list
    ]
    actual_input_paths = list(filter(os.path.exists, potential_input_paths))
    if potential_input_paths != actual_input_paths:
        logging.warning(
            "The following expected output files were not generated by `digital-land pipeline`: {}".format(
                set(potential_input_paths).difference(actual_input_paths)
            )
        )

    dataset_path = os.path.join(
        collection_repository_path,
        "dataset",
    )
    os.makedirs(dataset_path)
    sqlite_artifact_path = os.path.join(
        dataset_path,
        f"{pipeline_name}.sqlite3",
    )
    unified_collection_csv_path = os.path.join(
        dataset_path,
        f"{pipeline_name}.csv",
    )

    logging.info (
        f"digital-land --pipeline-name {pipeline_name} load-entries "
        f" {actual_input_paths} {sqlite_artifact_path}"
    )

    api.load_entries_cmd(
        actual_input_paths,
        sqlite_artifact_path
    )

    logging.info (
        f"digital-land --pipeline-name {pipeline_name} build-dataset "
        f" {sqlite_artifact_path} {unified_collection_csv_path}"
    )
    api.build_dataset_cmd(
        sqlite_artifact_path,
        unified_collection_csv_path
    )


@task(
    task_id="push_s3_collection"
)
def callable_push_s3_collection_task(**kwargs):
    if ENVIRONMENT != "production":
        raise AirflowSkipException(f"Doing nothing as $ENVIRONMENT is {ENVIRONMENT} and not 'production'")
    pipeline_name = kwargs['dag']._dag_id
    collection_repository_path = _get_collection_repository_path(kwargs)

    _upload_directory_to_s3(
        directory=os.path.join(
            collection_repository_path,
            "resource",
            pipeline_name,
        ),
        destination=f"{pipeline_name}/collection/resource"
    )

    _upload_files_to_s3(
        files=[
            "log.csv", "resource.csv", "source.csv", "endpoint.csv",
        ],
        directory=os.path.join(collection_repository_path, "collection"),
        destination=f"{pipeline_name}/collection/"
    )


@task(
    task_id="push_s3_dataset"
)
def callable_push_s3_dataset_task(**kwargs):
    if ENVIRONMENT != "production":
        raise AirflowSkipException(f"Doing nothing as $ENVIRONMENT is {ENVIRONMENT} and not 'production'")
    pipeline_name = kwargs['dag']._dag_id
    collection_repository_path = _get_collection_repository_path(kwargs)

    _upload_directory_to_s3(
        directory=os.path.join(
            collection_repository_path,
            "transformed",
            pipeline_name,
        ),
        destination=f"{pipeline_name}/transformed"
    )

    _upload_directory_to_s3(
        directory=os.path.join(
            collection_repository_path,
            "issue",
            pipeline_name,
        ),
        destination=f"{pipeline_name}/issue"
    )

    _upload_directory_to_s3(
        directory=os.path.join(
            collection_repository_path,
            "dataset",
            pipeline_name,
        ),
        destination=f"{pipeline_name}/dataset"
    )


def kebab_to_pascal_case(kebab_case_str):
    return pascalize(kebab_case_str.replace('-', '_'))


pipelines = [
    'listed-building',
    'brownfield-land',
]
for pipeline_name in pipelines:
    with DAG(
        pipeline_name,
        schedule_interval=timedelta(days=1),
        start_date=datetime.now()
    ) as InstantiatedDag:
        clone = callable_clone_task()
        download_s3_resources = callable_download_s3_resources_task()
        collect = callable_collect_task()
        collection = callable_collection_task()
        commit_collect = callable_commit_task()
        commit_collection = callable_commit_task()
        commit_harmonised = callable_commit_task()
        push_s3_collection = callable_push_s3_collection_task()
        dataset = callable_dataset_task()
        build_dataset = callable_build_dataset_task()
        push_s3_dataset = callable_push_s3_dataset_task()

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

        # Airflow likes to be able to find its DAG's as module scoped variables
        globals()[f"{kebab_to_pascal_case(pipeline_name)}Dag"] = InstantiatedDag
