FROM ubuntu:20.04

ARG spark_version=3.3.1

WORKDIR /home

# package 설치
RUN apt-get update && \
    apt-get install wget -y && \
    apt-get install python3.9 -y && \
    apt-get install python3-pip -y && \
    apt-get install openjdk-11-jdk -y

# Spark 설치
RUN wget https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop3.tgz && \
    tar -xvf spark-${spark_version}-bin-hadoop3.tgz && \
    mkdir spark && \
    mv spark-${spark_version}-bin-hadoop3 spark && \
    rm -rf spark-${spark_version}-bin-hadoop3.tgz

RUN pip install pandas

# 환경변수 설정
ENV JAVA_HOME /Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home
ENV SPARK_HOME /home/spark 
ENV PATH $PATH:${JAVA_HOME}/bin:${SPARK_HOME}/bin

ENV SPARK_MASTER_HOST spark-master 
ENV SPARK_MASTER_PORT 7077