include makerules/makerules.mk

# TODO add this ECR repository to terraform
BUILD_REPO := public.ecr.aws/l6z6v3j6
BUILD_TAG := $(BUILD_REPO)/digital-land-airflow:latest

# We are baking resources (to be declarative) into the image so we don't want them cached
docker-build: docker-check
	docker build -t $(BUILD_TAG) . --no-cache

docker-login:
	aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws

docker-push: docker-check docker-login
	docker push $(BUILD_TAG)

docker-test: docker-check
	docker run -t --user airflow --entrypoint bash $(BUILD_TAG) -c 'pytest $$(python -c "import inspect, os; from digital_land_airflow import tests; print(os.path.dirname(inspect.getfile(tests)))") -p digital_land_airflow.tests.fixtures.base'

docker-check:
ifeq (, $(shell which docker))
	$(error "No docker in $(PATH), consider doing apt-get install docker OR brew install --cask docker")
endif

