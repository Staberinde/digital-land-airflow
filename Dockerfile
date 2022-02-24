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
