# Digital Land Airflow

[![Build](https://github.com/digital-land/digital-land-airflow/workflows/Continuous%20Integration/badge.svg)](https://github.com/digital-land/digital-land-airflow/actions?query=workflow%3A%22Continuous+Integration%22)
[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/digital-land/digital-land-airflow/blob/main/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://black.readthedocs.io/en/stable/)
[![codecov](https://codecov.io/gh/digital-land/digital-land-airflow/branch/master/graph/badge.svg?token=HXKOXMILGB)](https://codecov.io/gh/digital-land/digital-land-airflow)

<!-- vim-markdown-toc Marked -->

* [Overview](#overview)
  * [Note on Terminology](#note-on-terminology)
* [Prerequisites](#prerequisites)
* [Setting Digital Land Airflow up locally](#setting-digital-land-airflow-up-locally)
  * [Debugging pipelines](#debugging-pipelines)
* [Using this repository and AWS authentication matters](#using-this-repository-and-aws-authentication-matters)

<!-- vim-markdown-toc -->

## Overview

This repository contains the [Apache Airflow](https://airflow.apache.org/) [DAG](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html)'s used in running the Digital Land pipeline (ETL) infrastructure.

### Note on Terminology

The term `pipeline` is used in this document to encompass both the updating and processing of a collection, which different from the use in `digital-land-python` where `pipeline` tends to refer to just the processing of a collection

## Prerequisites

* Docker
* Docker Compose

## Setting Digital Land Airflow up locally

* At the time of writing, it is necessary to be authenticated with the Digital Land AWS account in order to download pre-processed resource files generated by previous pipeline runs, in order to avoid having to regenerate these resources.
  * See [the section below](#using-this-repository-and-aws-authentication-matters) for advice on how to set this authentication up. The rest of the instructions will assume you are following this process.
* Running `aws-vault exec dl-dev -- docker-compose up` from the root of this repository should bring up airflow and allow you to access the [Airflow UI](https://airflow.apache.org/docs/apache-airflow/stable/ui.html) at http://localhost:8080 from where you can enable and trigger workflows.
* To trigger a workflow from the command line, you can run:

```sh
aws-vault exec dl-dev -- ./airflow.sh dags trigger <workflow name>
```

e.g.

```sh
aws-vault exec dl-dev -- ./airflow.sh dags trigger listed-building
```

Note that this won't run the pipeline synchronously; you'll need to have airflow running via `aws-vault exec dl-dev -- docker-compose up` in order for the pipeline to execute.

### Debugging pipelines

If you need to inspect the working directory (i.e. the collection repository checked out from git, with the resources pulled from S3 and any changes arising from the execution) of a failed pipeline, run:

```
docker-compose exec airflow-worker bash
```

from the `digital-land-airflow` root directory (i.e. the same directory as this README.md)

You should then be able to find the working directory under the path `/tmp/{pipeline name}_manual__{ISO 8601 timestamp of execution start}/{pipeline name}-collection/`

Alternatively, if you want to copy the working directory to your host system, run:

```
docker cp $(docker-compose ps -q airflow-worker):/tmp/{pipeline name}_manual__{ISO 8601 timestamp of execution start}/{pipeline name}-collection ../airflow-collection-working-directory
```

from the `digital-land-airflow` root directory (i.e. the same directory as this README.md)

Or to avoid having to look for the execution timestamp you could just run something like

```
docker cp $(docker-compose ps -q airflow-worker):/tmp ./airflow-execution-tmp
```

## Using this repository and AWS authentication matters

Throughout this guide, [aws-vault](https://github.com/99designs/aws-vault) is used in order to assume the correct role for accessing our AWS environment.
It is recommended to set something like this up, but you can get by with manual authentication or other tooling. Just ensure that the
various AWS env vars are setup such that you can use the aws cli as the `developer` role. You can check this with the following command:

```bash
aws sts get-caller-identity
```

If everything is configured correctly this should return the details of the `developer` role.

```json
{
    "UserId": "[USER_ID]",
    "Account": "[ACCOUNT_ID]",
    "Arn": "arn:aws:sts::[ACCOUNT_ID]:assumed-role/developer/[BIGNUM]"
}
```

Commands in the rest of this readme assume you've setup a profile in aws-vault with the name dl-dev. If you name the profile something else, you'll need to adjust the commands.
