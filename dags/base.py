import logging
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import boto3
from git import Repo
from humps import pascalize

from digital_land.api import DigitalLandApi
from digital_land.specification import specification_path


ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")


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
    dag = kwargs['dag']
    ti = kwargs['ti']
    pipeline_name = dag._dag_id

    collection_repository_path = ti.xcom_pull(key="collection_repository_path")
    pipeline_dir = os.path.join(collection_repository_path, 'pipeline')
    assert os.path.exists(pipeline_dir)

    logging.info(
        f"Instantiating DigitalLandApi for pipeline {pipeline_name} using pipeline "
        f"directory: {pipeline_dir} and specification_directory {specification_path}"
    )
    api = DigitalLandApi(
        debug=False,
        pipeline_name=pipeline_name,
        pipeline_dir=pipeline_dir,
        specification_dir=specification_path
    )

    endpoint_path = os.path.join(collection_repository_path, "collection/endpoint.csv")
    collection_dir = os.path.join(collection_repository_path, 'collection')

    logging.info(
        f"Calling collect_cmd with endpoint_path {endpoint_path} and collection_dir {collection_dir}"
    )

    api.collect_cmd(
        endpoint_path=endpoint_path,
        collection_dir=collection_dir
    )
    ti.xcom_push("api_instance", api.to_json())


@task(
    task_id="download_s3_resources"
)
def callable_download_s3_resources_task(**kwargs):
    dag = kwargs['dag']
    ti = kwargs['ti']
    pipeline_name = dag._dag_id
    collection_s3_bucket = Variable.get("collection_s3_bucket")
    collection_repository_path = ti.xcom_pull(key="collection_repository_path")

    s3 = boto3.resource("s3")
    s3.meta.client.download_file(
        collection_s3_bucket,
        f"{pipeline_name}/collection/resource/",
        os.path.join(collection_repository_path, "collection", "resource")
    )


@task(
    task_id="collection",
)
def callable_collection_task(**kwargs):
    ti = kwargs['ti']
    api_constructor_args = json.loads(ti.xcom_pull(key="api_instance"))

    logging.info(
        f"Instantiating DigitalLandApi for pipeline {api_constructor_args['pipeline_name']} using pipeline "
        f"directory: {api_constructor_args['pipeline_dir']} and "
        f"specification_directory {api_constructor_args['specification_dir']}"
    )
    api = DigitalLandApi(**api_constructor_args)
    collection_repository_path = ti.xcom_pull(key="collection_repository_path")

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
    # TODO replace with branch python operator
    if ENVIRONMENT != "production":
        logging.info(f"Doing nothing as $ENVIRONMENT is {ENVIRONMENT} and not 'production'")
        return
    ti = kwargs['ti']
    repo = ti.xcom_pull(key="collection_repository_path")
    repo = Repo(repo)
    repo.git.add(update=False)
    repo.index.commit(f"Data {datetime.now().isoformat()}")
    repo.remotes["origin"].push()


@task(
    task_id="dataset",
)
def callable_dataset_task(**kwargs):
    dag = kwargs['dag']
    ti = kwargs['ti']
    pipeline_name = dag._dag_id

    collection_repository_path = ti.xcom_pull(key="collection_repository_path")
    pipeline_dir = os.path.join(collection_repository_path, 'pipeline')
    assert os.path.exists(pipeline_dir)

    logging.info(
        f"Instantiating DigitalLandApi for pipeline {pipeline_name} using pipeline "
        f"directory: {pipeline_dir} and specification_directory {specification_path}"
    )
    api = DigitalLandApi(
        debug=False,
        pipeline_name=pipeline_name,
        pipeline_dir=pipeline_dir,
        specification_dir=specification_path
    )

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
    dag = kwargs['dag']
    ti = kwargs['ti']
    pipeline_name = dag._dag_id

    collection_repository_path = ti.xcom_pull(key="collection_repository_path")
    pipeline_dir = os.path.join(collection_repository_path, 'pipeline')
    assert os.path.exists(pipeline_dir)

    logging.info(
        f"Instantiating DigitalLandApi for pipeline {pipeline_name} using pipeline "
        f"directory: {pipeline_dir} and specification_directory {specification_path}"
    )
    api = DigitalLandApi(
        debug=False,
        pipeline_name=pipeline_name,
        pipeline_dir=pipeline_dir,
        specification_dir=specification_path
    )

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
        logging.info(f"Doing nothing as $ENVIRONMENT is {ENVIRONMENT} and not 'production'")
        return
    dag = kwargs['dag']
    ti = kwargs['ti']
    pipeline_name = dag._dag_id
    collection_s3_bucket = Variable.get("collection_s3_bucket")
    collection_repository_path = ti.xcom_pull(key="collection_repository_path")
    s3 = boto3.resource("s3")

    resource_dir = os.path.join(
        collection_repository_path,
        "resource",
        pipeline_name,
    )
    resource_files = os.listdir(resource_dir)
    for file_to_upload in resource_files:
        s3.meta.client.upload_file(
            os.path.join(resource_dir, file_to_upload),
            collection_s3_bucket,
            f"{pipeline_name}/collection/resource/{file_to_upload}",
        )

    collection_csv_files = [
        "log.csv", "resource.csv", "source.csv", "endpoint.csv",
    ]
    for filename in collection_csv_files:
        s3.meta.client.upload_file(
            os.path.join(collection_repository_path, "collection", filename),
            collection_s3_bucket,
            f"{pipeline_name}/collection/{filename}",
        )


@task(
    task_id="push_s3_dataset"
)
def callable_push_s3_dataset_task(**kwargs):
    if ENVIRONMENT != "production":
        logging.info(f"Doing nothing as $ENVIRONMENT is {ENVIRONMENT} and not 'production'")
        return 
    dag = kwargs['dag']
    ti = kwargs['ti']
    pipeline_name = dag._dag_id
    collection_s3_bucket = Variable.get("collection_s3_bucket")
    collection_repository_path = ti.xcom_pull(key="collection_repository_path")
    s3 = boto3.resource("s3")

    # TODO refactor these into method
    transformed_dir = os.path.join(
        collection_repository_path,
        "transformed",
        pipeline_name,
    )
    transformed_files = os.listdir(transformed_dir)
    for file_to_upload in transformed_files:
        s3.meta.client.upload_file(
            os.path.join(transformed_dir, file_to_upload),
            collection_s3_bucket,
            f"{pipeline_name}/transformed/{file_to_upload}",
        )

    issue_dir = os.path.join(
        collection_repository_path,
        "issues",
        pipeline_name,
    )
    issue_files = os.listdir(issue_dir)
    for file_to_upload in issue_files:
        s3.meta.client.upload_file(
            os.path.join(issue_dir, file_to_upload),
            collection_s3_bucket,
            f"{pipeline_name}/issue/{file_to_upload}",
        )
    dataset_dir = os.path.join(
        collection_repository_path,
        "dataset",
        pipeline_name,
    )
    dataset_files = os.listdir(dataset_dir)
    for file_to_upload in dataset_files:
        s3.meta.client.upload_file(
            os.path.join(dataset_dir, file_to_upload),
            collection_s3_bucket,
            f"{pipeline_name}/dataset/{file_to_upload}",
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
        # TODO make commit's their own graph branch
        commit_collect >> collection
        collection >> commit_collection
        commit_collection >> push_s3_collection
        push_s3_collection >> dataset
        dataset >> build_dataset
        build_dataset >> commit_harmonised
        commit_harmonised >> push_s3_dataset

        # Airflow likes to be able to find its DAG's as module scoped variables
        globals()[f"{kebab_to_pascal_case(pipeline_name)}Dag"] = InstantiatedDag
