from datetime import datetime
import logging
from shutil import rmtree

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from cloudpathlib import CloudPath
from git import Repo
from digital_land_airflow.tasks.utils import (
    get_collection_repository_path,
    get_environment,
    get_pipeline_resource_mapping,
    get_repo_name,
    get_resources_name,
    get_run_temporary_directory,
    upload_directory_to_s3,
    upload_files_to_s3
)


def callable_clone_task(**kwargs):
    ref_to_checkout = kwargs.get("params", {}).get("git_ref", "HEAD")
    repo_name = get_repo_name(kwargs)
    repo_path = get_run_temporary_directory(kwargs).joinpath(repo_name)

    # If we rerun a task within the same DAGrun, it will try an reuse the same path
    # We can't really let it use the same repo state right now as it won't match other
    # notions of state e.g. S3
    if repo_path.exists():
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


def callable_download_s3_resources_task(**kwargs):
    download_from_s3(
        "/collection/resource",
        get_collection_repository_path(kwargs).joinpath("collection").joinpath(
            "resource"
        ),
        **kwargs
    )


def download_from_s3(s3_path, destination_dir, **kwargs):
    s3_resource_name = get_resources_name(kwargs)
    collection_s3_bucket = Variable.get("collection_s3_bucket")

    s3_resource_path = f"s3://{collection_s3_bucket}/{s3_resource_name}/{s3_path}"
    destination_dir.mkdir(parents=True, exist_ok=True)
    cp = CloudPath(s3_resource_path)
    cp.download_to(destination_dir)
    logging.info(
        f"Copied resources from {s3_resource_path} to {destination_dir} . Got: {list(destination_dir.iterdir())}"
    )


def callable_commit_task(**kwargs):
    if kwargs.get("params", {}).get("specified_resources", []):
        raise AirflowSkipException(
            "Doing nothing as params['specified_resources'] is set"
        )
    environment = get_environment()
    if environment != "production":
        raise AirflowSkipException(
            f"Doing nothing as $ENVIRONMENT is {environment} and not 'production'"
        )
    paths_to_commit = kwargs["paths_to_commit"]
    collection_repository_path = get_collection_repository_path(kwargs)
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


def callable_push_s3_task(**kwargs):
    repo_name = get_repo_name(kwargs)
    environment = get_environment()
    if environment not in ["production", "staging"]:
        raise AirflowSkipException(
            f"Doing nothing as $ENVIRONMENT is {environment} and not 'production' or 'staging'"
        )
    if kwargs.get("params", {}).get("git_ref", "HEAD") != "HEAD":
        raise AirflowSkipException(
            "Doing nothing as params['git_ref'] is set and we won't be able to push unless we're at HEAD"
        )
    collection_repository_path = get_collection_repository_path(kwargs)
    pipeline_resource_mapping = get_pipeline_resource_mapping(kwargs)
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
            upload_directory_to_s3(
                directory=collection_repository_path.joinpath(source_directory),
                destination=destination_directory,
            )

        for source_files, destination_directory in files_to_push:
            upload_files_to_s3(
                files=[
                    collection_repository_path.joinpath(filepath)
                    for filepath in source_files
                ],
                destination=destination_directory,
            )


def callable_working_directory_cleanup_task(**kwargs):
    collection_repository_path = get_collection_repository_path(kwargs)
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
