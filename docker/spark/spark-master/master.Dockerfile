FROM spark-base

ARG spark_master_web_ui=8080
ARG spark_version

WORKDIR /home/spark/spark-${spark_version}-bin-hadoop3

EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}
CMD bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out