FROM puckel/docker-airflow:1.10.2
MAINTAINER OCTO DD team <datadriver@octo.com>

SHELL ["/bin/bash", "-c"]
ENV AIRFLOW_GPL_UNIDECODE=yes
ENV MINIO_ACCESS_KEY=key_test
ENV MINIO_SECRET_KEY=secret_test

USER root

RUN apt-get update -y

RUN apt-get -y install git wget pandoc awscli \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

RUN pip install git+https://github.com/Anaconda-Server/anaconda-client && anaconda --version \
    && pip install virtualenv

USER airflow