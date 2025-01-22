FROM apache/airflow:2.9.2
FROM openjdk:8

USER root
RUN apt-get update
RUN install -y openjdk-8-jdk
RUN apt-get autoremove -yqq --purge
RUN apt-get install -y wget
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

RUN wget https://download.oracle.com/java/23/latest/jdk-23_linux-x64_bin.tar.gz && \ 
    mkdir -p /opt/java && \ 
    tar -xvf jdk-23_linux-x64_bin.tar.gz -C /opt/java && \ 
    rm jdk-23_linux-x64_bin.tar.gz

RUN wget -O jre-8u421-linux-x64.tar.gz https://javadl.oracle.com/webapps/download/AutoDL?BundleId=250118_d8aa705069af427f9b83e66b34f5e380 && \
    tar -xvf jre-8u421-linux-x64.tar.gz -C /opt/java && \
    rm jre-8u421-linux-x64.tar.gz

RUN wget https://downloads.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    mkdir -p /opt/spark && \
    tar -xvf spark-3.5.4-bin-hadoop3.tgz -C /opt/spark && \
    rm spark-3.5.4-bin-hadoop3.tgz

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME
ENV SPARK_HOME=/opt/spark/spark-3.5.4-bin-hadoop3
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

COPY requirements.txt /requirements.txt
RUN chmod 777 /requirements.txt

USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt