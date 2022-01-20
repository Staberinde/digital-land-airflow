import os
from datetime import datetime
from tempfile import TemporaryDirectory

from airflow import DAG
from airflow.decorators import task
from airflow.models.variable import Variable

ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")


@task.virtualenv(
    task_id="clone",
    requirements=[
        "GitPython",
    ],
    system_site_packages=False
)
def callable_clone_task():
    import os
    from git import Repo

    pipeline_name = Variable.get("pipeline_name")
    tempdir = Variable.get("dag_temp_dir")
    repo_name = f"{pipeline_name}-collection"
    repo_path = os.path.join(tempdir, repo_name)
    repo = Repo.clone_from(f"https://github.com/digital-land/{repo_name}", to_path=repo_path)
    task.xcom_push("collection_repository", repo)
    task.xcom_push("collection_repository_path", repo_path)


@task.virtualenv(
    task_id="collect",
    requirements=[
        "git+https://github.com/digital-land/digital-land-python@refactor-cli-into-class",
        "git+https://github.com/digital-land/specification"
    ],
    system_site_packages=False
)
def callable_collect_task():
    import os
    from specification import get_specification_path
    from digital_land.api import API

    pipeline_name = Variable.get("pipeline_name")
    collection_repository_path = task.xcom_pull("collection_repository_path")
    api = API(
        debug=False,
        pipeline_name=pipeline_name,
        pipeline_dir=collection_repository_path,
        specification_dir=get_specification_path()
    )
    task.xcom_push("api_instance", api)
    api.collect_cmd(
        endpoint_path=os.path.join(collection_repository_path, "collection/endpoint.csv"),
        collection_dir=collection_repository_path
    )


def sync_s3():
    pass
    # TODO implement something along lines of https://airflow.apache.org/docs/apache-airflow/1.10.2/integration.html?highlight=s3#s3hook here


@task.virtualenv(
    task_id="collection",
    requirements=[
        "git+https://github.com/digital-land/digital-land-python@refactor-cli-into-class",
        "git+https://github.com/digital-land/specification"
    ],
    system_site_packages=False
)
def callable_collection_task():

    api = task.xcom_pull("api_instance")
    collection_repository_path = task.xcom_pull("collection_repository_path")
    api.pipeline_collection_save_csv_cmd(
        collection_dir=collection_repository_path
    )


@task.virtualenv(
    task_id="commit",
    requirements=[
        "gitPython"
    ],
    system_site_packages=False
)
def callable_commit_task():
    repo = task.xcom_pull("collection_repository")
    # TODO replace with branch python operator
    if ENVIRONMENT == "production":
        repo.index.commit("initial commit")
        repo.remotes["origin"].push()


@task.virtualenv(
    task_id="dataset",
    requirements=[
    ],
    system_site_packages=False
)
def callable_dataset_task():
    pass
    #TODO what does `make dataset` actually do?!?!? There's no build target, only build-dataset



pipeline_name = 'listed-buildings'
with (
    DAG(
        pipeline_name,
        start_date=datetime.now()
    ) as BaseDag,
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
