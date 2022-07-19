from csv import DictReader
from datetime import datetime
import os
from pathlib import Path

from humps import pascalize
from pendulum.tz import timezone
from digital_land.specification import Specification, specification_path

from digital_land_airflow.tasks.utils import get_environment

DATE_DEPLOYED_ON_STAGING = datetime(2022, 3, 11, 0, 0, tzinfo=timezone("Europe/London"))


def get_dag_cronstring():
    return os.environ.get("PIPELINE_RUN_CRON_STRING")


def get_dag_start_date():
    environment = get_environment()
    if environment == "staging":
        return DATE_DEPLOYED_ON_STAGING
    else:
        return datetime.now()


def kebab_to_pascal_case(kebab_case_str):
    return pascalize(kebab_case_str.replace("-", "_"))


def get_all_collection_names():
    return [
        collection["collection"]
        for collection in DictReader(
            Path(specification_path).joinpath("collection.csv").open()
        )
    ]


def get_datasets_from_collection(collection_name: str) -> filter:
    spec = Specification(specification_path)
    return filter(
        lambda dataset_name: spec.dataset[dataset_name]["collection"] == collection_name,
        spec.dataset_names
    )
