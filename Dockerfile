FROM apache/airflow:2.2.3-python3.9

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

USER airflow

COPY ./setup.py setup.py
RUN pip install --upgrade .[test]
