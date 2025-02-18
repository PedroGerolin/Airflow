FROM apache/airflow:2.10.3-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
#COPY ./airflow.cfg /opt/airflow/airflow.cfg
#RUN chown airflow. airflow.cfg
USER airflow

COPY config/requirements.txt /usr/local/airflow/dags/requirements.txt

RUN pip install --no-cache-dir -r /usr/local/airflow/dags/requirements.txt  --use-deprecated=legacy-resolver
