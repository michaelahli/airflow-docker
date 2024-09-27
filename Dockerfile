FROM apache/airflow:2.10.2-python3.9
COPY requirements.txt /requirements.txt
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    heimdal-dev \
    default-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
COPY jars /opt/airflow/jars
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install "apache-airflow[password,celery,mysql,jdbc,apache.hive,apache.spark,apache.hdfs]==${AIRFLOW_VERSION}"