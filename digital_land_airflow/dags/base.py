from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Param
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum.tz import timezone

from digital_land_airflow.dags.utils import (
    get_all_collection_names,
    _get_dag_cronstring,
    _get_dag_start_date,
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
    is_run_harmonised_stage
)


for collection_name in get_all_collection_names():
    with DAG(
        collection_name,
        start_date=_get_dag_start_date(),
        timetable=CronDataIntervalTimetable(
            _get_dag_cronstring(), timezone=timezone("Europe/London")
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

        if is_run_harmonised_stage(collection_name):
            commit_harmonised = PythonOperator(
                task_id="commit_harmonised",
                python_callable=callable_commit_task,
                op_kwargs={"paths_to_commit": ["harmonised"]},
            )
            build_dataset >> commit_harmonised
            commit_harmonised >> working_directory_cleanup

        # Airflow likes to be able to find its DAG's as module scoped variables
        globals()[f"{kebab_to_pascal_case(collection_name)}Dag"] = InstantiatedDag
