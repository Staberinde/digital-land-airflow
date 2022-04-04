from csv import DictReader
import logging
import os
from datetime import datetime
from pathlib import Path
from shutil import rmtree

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.models import Param, Variable
from airflow.timetables.interval import CronDataIntervalTimetable
import boto3
from cloudpathlib import CloudPath
from git import Repo
from humps import pascalize
import requests
from pendulum.tz import timezone

from digital_land.api import DigitalLandApi
from digital_land.specification import specification_path


DATE_DEPLOYED_ON_STAGING = datetime(2022, 3, 11, 0, 0, tzinfo=timezone("Europe/London"))


class ResourceNotFound(Exception):
    """Raised if explicitly specified resource cannot be found in resources/"""

    pass


def _get_environment():
    return os.environ.get("ENVIRONMENT", "development")


def _get_dag_cronstring():
    return os.environ.get("PIPELINE_RUN_CRON_STRING")


def _get_dag_start_date():
    environment = _get_environment()
    if environment == "staging":
        return DATE_DEPLOYED_ON_STAGING
    else:
        return datetime.now()


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


def callable_clone_task(**kwargs):
    ref_to_checkout = kwargs.get("params", {}).get("git_ref", "HEAD")
    repo_name = _get_repo_name(kwargs)
    repo_path = _get_run_temporary_directory(kwargs).joinpath(repo_name)

    # If we rerun a task within the same DAGrun, it will try an reuse the same path
    # We can't really let it use the same repo state right now as it won't match other
    # notions of state e.g. S3
    if kwargs.get("params", {}).get("prev_attempted_tries", 1) > 1 and repo_path.exists():
        logging.info(f"Removing existing directory on path {repo_path}")
        rmtree(repo_path)

    repo_path.mkdir(parents=True)
    repo = Repo.clone_from(
        f"https://github.com/digital-land/{repo_name}", to_path=repo_path
    )
    if ref_to_checkout != "HEAD":
        logging.info(f"Checking out git ref {ref_to_checkout}")
        new_branch = repo.create_head("new")
        new_branch.commit = ref_to_checkout
        new_branch.checkout()

    kwargs["ti"].xcom_push("collection_repository_path", str(repo_path))


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


def callable_download_s3_resources_task(**kwargs):
    s3_resource_name = _get_resources_name(kwargs)
    collection_s3_bucket = Variable.get("collection_s3_bucket")
    collection_repository_path = _get_collection_repository_path(kwargs)

    s3_resource_path = (
        f"s3://{collection_s3_bucket}/{s3_resource_name}/collection/resource/"
    )
    destination_dir = collection_repository_path.joinpath("collection").joinpath(
        "resource"
    )
    destination_dir.mkdir(parents=True, exist_ok=True)
    cp = CloudPath(s3_resource_path)
    cp.download_to(destination_dir)
    logging.info(
        f"Copied resources from {s3_resource_path} to {destination_dir} . Got: {list(destination_dir.iterdir())}"
    )


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


def callable_commit_task(**kwargs):
    if kwargs.get("params", {}).get("specified_resources", []):
        raise AirflowSkipException(
            "Doing nothing as params['specified_resources'] is set"
        )
    environment = _get_environment()
    if environment != "production":
        raise AirflowSkipException(
            f"Doing nothing as $ENVIRONMENT is {environment} and not 'production'"
        )
    paths_to_commit = kwargs["paths_to_commit"]
    collection_repository_path = _get_collection_repository_path(kwargs)
    repo = Repo(collection_repository_path)

    if kwargs.get("params", {}).get("git_ref", "HEAD") != "HEAD":
        raise AirflowSkipException(
            "Doing nothing as params['git_ref'] is set and we won't be able to push unless we're at HEAD"
        )
    logging.info(f"Staging {paths_to_commit} for commit")
    repo.git.add(*paths_to_commit)
    # Assert every change staged
    diff_against_staged = repo.index.diff(None)
    assert len(diff_against_staged) == 0, list(map(str, diff_against_staged))

    commit_message = f"Data {datetime.now().isoformat()}"
    logging.info(f"Creating commit {commit_message}")
    repo.index.commit(commit_message)

    upstream_urls = list(repo.remotes["origin"].urls)
    assert len(upstream_urls) == 1, upstream_urls
    repo.remotes["origin"].push()
    logging.info(f"Commit {commit_message} pushed to {upstream_urls[0]}")


def callable_dataset_task(**kwargs):
    collection_name = _get_collection_name(kwargs)
    save_harmonised = is_run_harmonised_stage(collection_name)
    api = _get_api_instance(kwargs)
    collection_repository_path = _get_collection_repository_path(kwargs)

    collection_dir = collection_repository_path.joinpath("collection")
    organisation_csv_path = _get_organisation_csv(kwargs)
    resource_dir = collection_dir.joinpath("resource")

    resource_pipeline_mapping = _get_resource_pipeline_mapping(kwargs)
    assert len(resource_pipeline_mapping) > 0
    for resource_hash, dataset_names in resource_pipeline_mapping.items():
        for dataset_name in dataset_names:
            api = _get_api_instance(kwargs, dataset_name=dataset_name)
            issue_dir = collection_repository_path.joinpath("issue").joinpath(
                dataset_name
            )
            issue_dir.mkdir(exist_ok=True, parents=True)
            collection_repository_path.joinpath("transformed").joinpath(
                dataset_name
            ).mkdir(exist_ok=True, parents=True)
            if save_harmonised:
                collection_repository_path.joinpath("harmonised").joinpath(
                    dataset_name
                ).mkdir(exist_ok=True, parents=True)

            # These are hard coded relative paths in digital-land-python
            column_field_dir = collection_repository_path.joinpath(
                "var/column-field/"
            ).joinpath(dataset_name)
            column_field_dir.mkdir(exist_ok=True, parents=True)
            dataset_resource_dir = collection_repository_path.joinpath(
                "var/dataset-resource"
            ).joinpath(dataset_name)
            dataset_resource_dir.mkdir(exist_ok=True, parents=True)
            # Most digital_land.API() commands expect strings not pathlib.Path
            pipeline_cmd_args = {
                "input_path": str(resource_dir.joinpath(resource_hash)),
                "output_path": str(
                    collection_repository_path.joinpath("transformed")
                    .joinpath(dataset_name)
                    .joinpath(f"{resource_hash}.csv")
                ),
                "collection_dir": collection_dir,
                "null_path": None,
                "issue_dir": issue_dir,
                "organisation_path": organisation_csv_path,
                # TODO Figure out a way to do this without hardcoding, maybe introspect collection filesystem?
                "save_harmonised": save_harmonised,
                "column_field_dir": str(column_field_dir),
                "dataset_resource_dir": str(dataset_resource_dir),
                "custom_temp_dir": str(_get_temporary_directory())
            }
            log_string = (
                f"digital-land --pipeline-name {dataset_name} pipeline "
                f"--issue-dir {pipeline_cmd_args['issue_dir']} "
                f" {pipeline_cmd_args['input_path']} {pipeline_cmd_args['output_path']} "
                f"--organisation-path {pipeline_cmd_args['organisation_path']} "
                f"--column_field_dir {pipeline_cmd_args['column_field_dir']} "
                f"--dataset_resource_dir {pipeline_cmd_args['dataset_resource_dir']} "
            )
            if pipeline_cmd_args["null_path"]:
                log_string += f" --null-path {pipeline_cmd_args['null_path']}"
            if pipeline_cmd_args["save_harmonised"]:
                log_string += " --save-harmonised"

            logging.info(log_string)

            api.pipeline_cmd(**pipeline_cmd_args)


def callable_build_dataset_task(**kwargs):
    collection_repository_path = _get_collection_repository_path(kwargs)

    dataset_path = collection_repository_path.joinpath("dataset")
    dataset_path.mkdir()
    organisation_csv_path = _get_organisation_csv(kwargs)

    pipeline_resource_mapping = _get_pipeline_resource_mapping(kwargs)
    assert len(pipeline_resource_mapping) > 0
    for dataset_name, resource_hash_list in pipeline_resource_mapping.items():
        api = _get_api_instance(kwargs, dataset_name=dataset_name)
        potential_input_paths = [
            collection_repository_path.joinpath("transformed")
            .joinpath(dataset_name)
            .joinpath(f"{resource_hash}.csv")
            for resource_hash in resource_hash_list
        ]
        actual_input_paths = list(filter(lambda x: x.exists(), potential_input_paths))
        if potential_input_paths != actual_input_paths:
            logging.warning(
                "The following expected output files were not generated by `digital-land pipeline`: {}".format(
                    set(potential_input_paths).difference(actual_input_paths)
                )
            )

        sqlite_artifact_path = dataset_path.joinpath(
            f"{dataset_name}.sqlite3",
        )
        unified_collection_csv_path = dataset_path.joinpath(
            f"{dataset_name}.csv",
        )
        # Most digital_land.API() commands expect strings not pathlib.Path
        actual_input_paths_str = list(map(str, actual_input_paths))
        sqlite_artifact_path_str = str(sqlite_artifact_path)
        unified_collection_csv_path_str = str(unified_collection_csv_path)

        logging.info(
            f"digital-land --pipeline-name {dataset_name} load-entries "
            f" {actual_input_paths_str} {sqlite_artifact_path_str}"
        )

        api.dataset_create_cmd(
            actual_input_paths_str, sqlite_artifact_path_str, organisation_csv_path
        )

        logging.info(
            f"digital-land --pipeline-name {dataset_name} build-dataset "
            f" {sqlite_artifact_path_str} {unified_collection_csv_path_str}"
        )
        api.dataset_dump_cmd(sqlite_artifact_path_str, unified_collection_csv_path_str)


def callable_push_s3_task(**kwargs):
    repo_name = _get_repo_name(kwargs)
    environment = _get_environment()
    if environment not in ["production", "staging"]:
        raise AirflowSkipException(
            f"Doing nothing as $ENVIRONMENT is {environment} and not 'production' or 'staging'"
        )
    if kwargs.get("params", {}).get("git_ref", "HEAD") != "HEAD":
        raise AirflowSkipException(
            "Doing nothing as params['git_ref'] is set and we won't be able to push unless we're at HEAD"
        )
    collection_repository_path = _get_collection_repository_path(kwargs)
    pipeline_resource_mapping = _get_pipeline_resource_mapping(kwargs)
    assert len(pipeline_resource_mapping) > 0
    for pipeline_name in pipeline_resource_mapping.keys():
        directories_to_push = [
            (
                local_directory_path.format(
                    repo_name=repo_name, pipeline_name=pipeline_name
                ),
                destination_directory_path.format(
                    repo_name=repo_name, pipeline_name=pipeline_name
                ),
            )
            for local_directory_path, destination_directory_path in kwargs[
                "directories_to_push"
            ]
        ]
        files_to_push = [
            (
                local_file_paths,
                destination_directory_path.format(
                    repo_name=repo_name, pipeline_name=pipeline_name
                ),
            )
            for local_file_paths, destination_directory_path in kwargs["files_to_push"]
        ]

        for source_directory, destination_directory in directories_to_push:
            _upload_directory_to_s3(
                directory=collection_repository_path.joinpath(source_directory),
                destination=destination_directory,
            )

        for source_files, destination_directory in files_to_push:
            _upload_files_to_s3(
                files=[
                    collection_repository_path.joinpath(filepath)
                    for filepath in source_files
                ],
                destination=destination_directory,
            )


def callable_working_directory_cleanup_task(**kwargs):
    collection_repository_path = _get_collection_repository_path(kwargs)
    if kwargs.get("params", {}).get(
        "delete_working_directory_on_pipeline_success", False
    ):
        logging.info(f"Removing directory structure {collection_repository_path}")
        rmtree(collection_repository_path)
        logging.info(
            f"Directory structure {collection_repository_path} removed successfully"
        )
    else:
        logging.info(
            f"Not removing directory structure {collection_repository_path} as "
            f"delete_working_directory_on_pipeline_success={kwargs['params']['delete_working_directory_on_pipeline_success']}"
        )


def kebab_to_pascal_case(kebab_case_str):
    return pascalize(kebab_case_str.replace("-", "_"))


def get_all_collection_names():
    return [
        collection["collection"]
        for collection in DictReader(
            Path(specification_path).joinpath("collection.csv").open()
        )
    ]


def is_run_harmonised_stage(collection_name):
    return "brownfield-land" in collection_name


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
