FROM apache/airflow:2.10.2-python3.9
COPY requirements.txt /requirements.txt
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    heimdal-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install "apache-airflow[password,celery,mysql,jdbc,apache.hive,apache.spark,apache.hdfs]==${AIRFLOW_VERSION}"