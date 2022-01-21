import json
import os
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

from airflow import DAG
from airflow.decorators import task
from git import Repo

from digital_land.api import DigitalLandApi
from digital_land.specification import specification_path


ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")

@task(
    task_id="clone",
)
def callable_clone_task(**kwargs):
    dag = kwargs['dag']
    run_id = kwargs['run_id']


    pipeline_name = dag._dag_id
    repo_name = f"{pipeline_name}-collection"
    tempdir = TemporaryDirectory(prefix=f"{pipeline_name}_{run_id}")
    repo_path = os.path.join(tempdir, repo_name)
    repo = Repo.clone_from(f"https://github.com/digital-land/{repo_name}", to_path=repo_path)
    task.xcom_push("collection_repository", repo)
    task.xcom_push("collection_repository_path", repo_path)
    task.xcom_push("dag_run_tempdir", tempdir)


@task(
    task_id="collect",
)
def callable_collect_task(**kwargs):
    dag = kwargs['dag']


    pipeline_name = dag.id
    collection_repository_path = task.xcom_pull("collection_repository_path")
    api = DigitalLandApi(
        debug=False,
        pipeline_name=pipeline_name,
        pipeline_dir=collection_repository_path,
        specification_dir=specification_path
    )
    task.xcom_push("api_instance", api)
    api.collect_cmd(
        endpoint_path=os.path.join(collection_repository_path, "collection/endpoint.csv"),
        collection_dir=collection_repository_path
    )


def sync_s3():
    pass
    # TODO implement something along lines of https://airflow.apache.org/docs/apache-airflow/1.10.2/integration.html?highlight=s3#s3hook here


@task(
    task_id="collection",
)
def callable_collection_task():

    api = DigitalLandApi(**json.loads(task.xcom_pull("api_instance")))
    collection_repository_path = task.xcom_pull("collection_repository_path")
    api.pipeline_collection_save_csv_cmd(
        collection_dir=collection_repository_path
    )


@task(
    task_id="commit",
)
def callable_commit_task():
    repo = task.xcom_pull("collection_repository")
    # TODO replace with branch python operator
    if ENVIRONMENT == "production":
        repo.index.commit("initial commit")
        repo.remotes["origin"].push()


@task(
    task_id="dataset",
)
def callable_dataset_task():
    pass
    #TODO what does `make dataset` actually do?!?!? There's no build target, only build-dataset




pipelines = [
    'listed-buildings',
]
for pipeline_name in pipelines:
    with (
        DAG(
            pipeline_name,
            schedule_interval=timedelta(days=1),
            start_date=datetime.now()
        ) as InstantiatedDag,
        TemporaryDirectory(prefix=f"{pipeline_name}_tempdir")
    ):
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
        globals()[f"{pipeline_name}Dag"] = InstantiatedDag
