ARG AIRFLOW_TAG
FROM apache/airflow:${AIRFLOW_TAG:-2.2.3-python3.9}

USER root

RUN set -xe; \
    apt-get update; \
    apt-get install -y \
        awscli \
        time \
        gosu \
        make \
        git \
        gdal-bin \
        libspatialite-dev \
        libsqlite3-mod-spatialite

RUN set -ex; \
    mkdir -p /static-files; \
    chown -R airflow /static-files

USER airflow

COPY ./ /opt/airflow/
# Airflow will look for dags relative to AIRFLOW_HOME (defaults to /opt/airflow) but we namespace under ./digital_land_airflow for python packaging
# Copying the dags and plugins twice seems like the least worst option for now
# TODO try and make this all work nicely with AIRFLOW_HOME=/opt/airflow/digital_land_airflow
# or add entrypoint in dags/__init__.py that imports contents of ./digital_land_airflow/dags/base.py (least prefered option)
COPY ./digital_land_airflow/dags/ /opt/airflow/dags/

RUN pip install --upgrade .[test,digital_land,dev]

ARG ORGANISATION_CSV_URL=https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv
RUN curl ${ORGANISATION_CSV_URL} > /static-files/organisation.csv
ENV ORGANISATION_CSV_PATH /static-files/organisation.csv
