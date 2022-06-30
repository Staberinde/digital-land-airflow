import logging

from digital_land_airflow.tasks.utils import (
    _get_api_instance,
    _get_collection_name,
    _get_resource_pipeline_mapping,
    _get_organisation_csv,
    is_run_harmonised_stage,
    _get_collection_repository_path,
    _get_pipeline_resource_mapping,
    _get_temporary_directory
)


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
        if not resource_dir.joinpath(resource_hash).exists():
            logging.warning(
                f"File with name {resource_hash} not present in collection/resource/ directory, "
                "likely this resource.csv entry was collected and committed in another environment, skipping.."
            )
            continue

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
                "custom_temp_dir": str(_get_temporary_directory()),
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
