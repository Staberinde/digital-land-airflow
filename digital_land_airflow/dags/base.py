import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Param, Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum.tz import timezone

from digital_land_airflow.dags.utils import (
    get_all_collection_names,
    get_dag_cronstring,
    get_dag_start_date,
    kebab_to_pascal_case,

)
from digital_land_airflow.tasks.collector import (
    callable_collect_task,
    callable_collection_task
)
from digital_land_airflow.tasks.filesystem import (
    callable_clone_task,
    callable_commit_task,
    callable_download_s3_resources_task,
    callable_push_s3_task,
    callable_working_directory_cleanup_task
)
from digital_land_airflow.tasks.pipeline import (
    callable_dataset_task,
    callable_build_dataset_task
)
from digital_land_airflow.tasks.utils import (
    is_run_harmonised_stage,
    get_temporary_directory
)

sensors = {}


def callable_pass_dataset_dir_to_builders(**kwargs):
    collection_repository_path = get_collection_repository_path(kwargs)

    dataset_path = collection_repository_path.joinpath("dataset")
    assert dataset_path.exists()
    kwargs["ti"].xcom_push("dataset_path", str(dataset_path))


def instantiate_dag(collection_name: str) -> DAG:
    with DAG(
        collection_name,
        start_date=get_dag_start_date(),
        timetable=CronDataIntervalTimetable(
            get_dag_cronstring(), timezone=timezone("Europe/London")
        ),
        catchup=False,
        render_template_as_native_obj=True,
        params={
            "git_ref": Param(
                type="string",
                help_text=(
                    "Commit of collection repository to run pipeline against. "
                    "Note this is for debugging and so will not push any artifacts to git or S3"
                ),
                default="HEAD",
            ),
            "resource_hashes": Param(
                type="array",
                help_text=(
                    "List of resource hashes to run pipeline against. "
                    "If specified, the collector will not be run"
                ),
                default=[],
            ),
            "delete_working_directory_on_pipeline_success": Param(
                type="boolean",
                help_text=(
                    "Denotes whether the working directory of the collection is deleted after successful DAG execution"
                ),
                default=True,
            ),
        },
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
            task_id="collection",
            python_callable=callable_collection_task,
            trigger_rule="none_failed",
        )
        commit_collect = PythonOperator(
            task_id="commit_collect",
            python_callable=callable_commit_task,
            op_kwargs={"paths_to_commit": ["collection/log"]},
        )
        commit_collection = PythonOperator(
            task_id="commit_collection",
            python_callable=callable_commit_task,
            op_kwargs={
                "paths_to_commit": ["collection/log.csv", "collection/resource.csv"]
            },
        )
        push_s3_collection = PythonOperator(
            task_id="push_s3_collection",
            python_callable=callable_push_s3_task,
            op_kwargs={
                "directories_to_push": [
                    ("collection/resource", "{repo_name}/collection/resource"),
                ],
                "files_to_push": [
                    (
                        [
                            "collection/endpoint.csv",
                            "collection/log.csv",
                            "collection/resource.csv",
                            "collection/source.csv",
                        ],
                        "{repo_name}/collection",
                    ),
                ],
            },
        )
        dataset = PythonOperator(
            task_id="dataset",
            python_callable=callable_dataset_task,
            trigger_rule="none_failed",
        )
        build_dataset = PythonOperator(
            task_id="build_dataset", python_callable=callable_build_dataset_task
        )
        push_s3_dataset = PythonOperator(
            task_id="push_s3_dataset",
            python_callable=callable_push_s3_task,
            op_kwargs={
                "directories_to_push": [
                    (
                        "transformed/{pipeline_name}",
                        "{repo_name}/transformed/{pipeline_name}",
                    ),
                    ("issue/{pipeline_name}", "{repo_name}/issue/{pipeline_name}"),
                    ("dataset", "{repo_name}/dataset"),
                ],
                "files_to_push": [],
            },
        )
        working_directory_cleanup = PythonOperator(
            task_id="working_directory_cleanup",
            python_callable=callable_working_directory_cleanup_task,
            trigger_rule="none_failed",
        )

        pass_dataset_dir_to_builders = PythonOperator(
            task_id="pass_dataset_dir_to_builders",
            python_callable=callable_pass_dataset_dir_to_builders,
            trigger_rule="none_failed",
        )

        clone >> download_s3_resources
        download_s3_resources >> collect
        collect >> commit_collect
        commit_collect >> collection
        collection >> commit_collection
        collection >> push_s3_collection
        commit_collection >> dataset
        push_s3_collection >> dataset
        dataset >> build_dataset
        build_dataset >> push_s3_dataset
        push_s3_dataset >> working_directory_cleanup
        working_directory_cleanup >> pass_dataset_dir_to_builders

        if is_run_harmonised_stage(collection_name):
            commit_harmonised = PythonOperator(
                task_id="commit_harmonised",
                python_callable=callable_commit_task,
                op_kwargs={"paths_to_commit": ["harmonised"]},
            )
            build_dataset >> commit_harmonised
            commit_harmonised >> working_directory_cleanup


        return InstantiatedDag


with DAG(
    "entity_builder",
    start_date=get_dag_start_date(),

) as entity_dag:

    for collection_name in get_all_collection_names():
        # Airflow likes to be able to find its DAG's as module scoped variables
        globals()[f"{kebab_to_pascal_case(collection_name)}Dag"] = instantiate_dag(collection_name)
        sensors[collection_name] = ExternalTaskSensor(
            task_id=f"entity_builder_trigger_for_{collection_name}",
            external_dag_id=collection_name,
            # Can leave out task_id maybe so it waits for dag?
            external_task_id=callable_pass_dataset_dir_to_builders.__name__,
            allowed_states=["success", "failed"]
        )

    entity_builder = DockerOperator(
        task_id="entity_builder",
        image="public.ecr.aws/l6z6v3j6/entity-builder",
        mount_tmp_dir=True,
        tmp_dir=str(get_temporary_directory()),
        tty=True,
        xcom_all=True,
        retrieve_output_path="/src/dataset/entity.sqlite3",
        environment={
            "COLLECTION_DATASET_BUCKET_NAME": Variable.get("collection_s3_bucket")
        },
        private_environment={
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_DEFAULT_REGION": os.environ["AWS_DEFAULT_REGION"],
            "AWS_REGION": os.environ["AWS_REGION"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_SECURITY_TOKEN": os.environ["AWS_SECURITY_TOKEN"],
            "AWS_SESSION_EXPIRATION": os.environ["AWS_SESSION_EXPIRATION"],
            "AWS_SESSION_TOKEN": os.environ["AWS_SESSION_TOKEN"],
        }
    )

    push_s3_dataset = PythonOperator(
        task_id="push_s3_dataset",
        python_callable=callable_push_s3_task,
        op_kwargs={
            "directories_to_push": [
                (
                    "entity-builder/dataset",
                    "entity-builder/dataset"
                ),
            ],
            "files_to_push": [],
        },
    )

    list(sensors.values()) >> entity_builder
    entity_builder >> push_s3_dataset
