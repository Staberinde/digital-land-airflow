import tempfile

from airflow import DAG
from airflow.decorators import task


@tempfile.TemporaryDirectory
class BaseDag(DAG):
    pipeline_name: str

    @task.virtualenv(
        task_id="clone",
        requirements=[
            "GitPython",
        ],
        system_site_packages=False
    )
    def clone(self, tempdir):
        import os
        from git import Repository

        repo_name = f"{self.pipeline_name}-collection"
        repo_path = os.path.join(tempdir, repo_name)
        repo = Repository.clone_from(f"https://github.com/digital-land/{repo_name}", to_path=repo_path)
        self.xcom_push("collection_repository", repo)
        self.xcom_push("collection_repository_path", repo_path)


    @task.virtualenv(
        task_id="collect",
        requirements=[
            "git+https://github.com/digital-land/digital-land-python",
            "git+https://github.com/digital-land/specification"
        ],
        system_site_packages=False
    )
    def collect(self, tempdir):
        import os
        from specification import get_specification_path
        from digital_land.api import API

        collection_repository_path = self.xcom_pull("collection_repository_path")
        api = API(
            debug=False,
            pipeline_name=self.pipeline_name,
            pipeline_dir=collection_repository_path,
            specification_dir=get_specification_path()
        )
        self.xcom_push("api_instance", api)
        api.collect_cmd(
            endpoint_path=os.path.join(collection_repository_path, "collection/endpoint.csv"),
            collection_dir=collection_repository_path
        )

    def sync_s3(self, tempdir):
        pass
        # TODO implement something along lines of https://airflow.apache.org/docs/apache-airflow/1.10.2/integration.html?highlight=s3#s3hook here

    @task.virtualenv(
        task_id="collection",
        requirements=[
            "git+https://github.com/digital-land/digital-land-python",
            "git+https://github.com/digital-land/specification"
        ],
        system_site_packages=False
    )
    def collection(self, tempdir):

        api = self.xcom_pull("api_instance")
        collection_repository_path = self.xcom_pull("collection_repository_path")
        api.pipeline_collection_save_csv_cmd(
            collection_dir=collection_repository_path
        )

    @task.virtualenv(
        task_id="commit-collection",
        requirements=[
            "gitPython"
        ],
        system_site_packages=False
    )
    def commit_collection(self, tempdir):
        repo = self.xcom_pull("collection_repository")
        repo.index.commit("initial commit")
        repo.remotes["origin"].push()


    @task.virtualenv(
        task_id="dataset",
        requirements=[
        ],
        system_site_packages=False
    )
    def dataset(self, tempdir):
        pass
        #TODO what does `make dataset` actually do?!?!? There's no build target, only build-dataset


