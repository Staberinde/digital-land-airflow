include makerules/makerules.mk

# TODO add this ECR repository to terraform
BUILD_REPO := public.ecr.aws/l6z6v3j6
GIT_COMMIT := $(shell git show -s --format=%H)
BUILD_IMAGE:= $(BUILD_REPO)/digital-land-airflow
BUILD_TAG := $(BUILD_IMAGE):$(GIT_COMMIT)

# We are baking resources (to be declarative) into the image so we don't want them cached
docker-build: docker-check
	docker build -t $(BUILD_TAG) . --no-cache
	docker tag $(BUILD_TAG) $(BUILD_IMAGE):latest

docker-login:
	aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws

docker-push: docker-check docker-login
	docker push $(BUILD_TAG)
	docker push $(BUILD_IMAGE):latest

docker-pull: docker-check docker-login
	docker pull $(BUILD_TAG)

docker-test: docker-check
	docker run -t --user airflow --entrypoint bash $(BUILD_TAG) -c 'pytest $$(python -c "import inspect, os; from digital_land_airflow import tests; print(os.path.dirname(inspect.getfile(tests)))") -p digital_land_airflow.tests.fixtures.base'

docker-check:
ifeq (,$(shell which docker))
	$(error "No docker in $(PATH), consider doing apt-get install docker OR brew install --cask docker")
endif

helm-check:
ifeq (,$(shell which helm))
	$(error "No helm in $(PATH), consider following apt install instructions https://helm.sh/docs/intro/install/#from-apt-debianubuntu OR snap install --classic helm OR brew install --cask helm")
endif
ifeq (,$(ENVIRONMENT))
	$(error "No environment specified via $$ENVIRONMENT, please pass as make argument")
endif

helm-login: helm-check
	# aws eks update-kubeconfig --region eu-west-2 --name $(ENVIRONMENT)-planning-data

helm-deploy: helm-login
ifeq (,$(shell docker manifest inspect $(BUILD_TAG)))
	$(error "Image for commit $(BUILD_TAG) does not exist in ECR registry. Please ensure commit is in upstream main, and that no tests have failed in `Build and deploy airflow DAG docker image` Github Action before attempting deploy")
endif
	helm repo add apache-airflow https://airflow.apache.org/
	helm repo update
	helm upgrade airflow-stable apache-airflow/airflow --namespace $(ENVIRONMENT)-pipelines --reuse-values --set images.airflow.tag=$(GIT_COMMIT) --kubeconfig ~/.kube/config
