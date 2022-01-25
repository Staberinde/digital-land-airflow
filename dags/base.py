import logging
import json
import os
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

from airflow import DAG
from airflow.decorators import task
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
    ti.xcom_push("api_instance", str(api))


def sync_s3():
    pass
    # TODO implement something along lines of https://airflow.apache.org/docs/apache-airflow/1.10.2/integration.html?highlight=s3#s3hook here


@task(
    task_id="collection",
)
def callable_collection_task(**kwargs):
    ti = kwargs['ti']
    api_constructor_args = json.loads(ti.xcom_pull(key="api_instance"))

    logging.info(
        f"Instantiating DigitalLandApi for pipeline {api_constructor_args['pipeline_name']} using pipeline "
        f"directory: {api_constructor_args['pipeline_dir']} and specification_directory {api_constructor_args['specification_dir']}"
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
    ti = kwargs['ti']
    repo = ti.xcom_pull(key="collection_repository_path")
    repo = Repo(repo)
    # TODO replace with branch python operator
    if ENVIRONMENT == "production":
        repo.index.commit("initial commit")
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

    endpoint_path = os.path.join(collection_repository_path, "collection/endpoint.csv")
    collection_dir = os.path.join(collection_repository_path, 'collection')

    logging.info(
        f"Calling collect_cmd with endpoint_path {endpoint_path} and collection_dir {collection_dir}"
    )

    api.dataset_cmd(
        endpoint_path=endpoint_path,
        collection_dir=collection_dir
    )
    ti.xcom_push("api_instance", str(api))





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
        collect = callable_collect_task()
        collection = callable_collection_task()
        commit_collect = callable_commit_task()
        commit_collection = callable_commit_task()
        clone >> collect
        collect >> commit_collect
        commit_collect >> collection
        collection >> commit_collection

        # Airflow likes to be able to find its DAG's as module scoped variables
        globals()[f"{kebab_to_pascal_case(pipeline_name)}Dag"] = InstantiatedDag
