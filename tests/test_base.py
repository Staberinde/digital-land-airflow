from datetime import datetime

from airflow.models import TaskInstance

from dags.base import callable_collect_task, ListedBuildingDag


def test_collect(requests_mock):
    task = callable_collect_task(dag=ListedBuildingDag).operator
    ti = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(ti.get_template_context())
