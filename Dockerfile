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

# We need this to match the executing UID as otherwise
# we can't use the python python packages we install here
# ARG EXECUTION_UID=1000

# RUN chown -R ${EXECUTION_UID} /opt/airflow
# USER ${EXECUTION_UID}
# ENV HOME=/opt/airflow
# ENV PATH=$PATH:/opt/airflow/.local/bin/
USER airflow

COPY ./setup.py setup.py
RUN pip install --upgrade .[test]

# USER root
# RUN chown -R ${EXECUTION_UID} /home/airflow
# USER ${EXECUTION_UID}
# ENV PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.9/site-packages/:opt/airflow/.local/lib/python3.9/site-packages/
