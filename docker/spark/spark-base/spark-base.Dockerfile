FROM ubuntu:20.04

ARG spark_version=3.3.2

WORKDIR /home

# apt 미러서버 미국(default) -> 한국 변경
RUN sed -i 's@archive.ubuntu.com@kr.archive.ubuntu.com@g' /etc/apt/sources.list

# package 설치
RUN apt-get update && \
    apt-get install wget -y && \
    apt-get install vim -y && \
    apt-get install python3.9 -y && \
    apt-get install python3-pip -y && \
    apt-get install openjdk-11-jdk -y

# Spark 설치
RUN wget https://dlcdn.apache.org/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop3.tgz && \
    tar -xvf spark-${spark_version}-bin-hadoop3.tgz && \
    mkdir spark && \
    mv spark-${spark_version}-bin-hadoop3 spark && \
    rm -rf spark-${spark_version}-bin-hadoop3.tgz && \
    mkdir /home/spark/spark-${spark_version}-bin-hadoop3/logs 

RUN pip3 install pandas && \
    pip3 install pyspark && \
    pip3 install py4j

# 환경변수 설정
ENV JAVA_HOME /usr
ENV SPARK_HOME /home/spark/spark-${spark_version}-bin-hadoop3
ENV PATH $PATH:${JAVA_HOME}/bin:${SPARK_HOME}/bin

ENV SPARK_MASTER_HOST spark-master 
ENV SPARK_MASTER_PORT 7077