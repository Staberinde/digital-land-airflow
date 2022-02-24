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

COPY . /opt/airflow
RUN pip install --upgrade .[test,digital_land,dev]

ARG ORGANISATION_CSV_URL=https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv
RUN curl ${ORGANISATION_CSV_URL} > /static-files/organisation.csv
ENV ORGANISATION_CSV_PATH /static-files/organisation.csv
