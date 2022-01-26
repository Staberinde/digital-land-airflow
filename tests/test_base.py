from datetime import datetime
from mock import Mock

from airflow.models import TaskInstance

from dags.base import callable_collect_task, ListedBuildingDag


def test_collect(requests_mock, mocker, tmpdir):

    kwargs = {
        "ti": Mock(
            **{
                "xcom_pull__return_value": tmpdir
            }
        ),
        "dag": Mock(**{"_dag_id": "listed-building"}),
    }
    response = callable_collect_task(**kwargs)
