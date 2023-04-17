FROM spark-base

ARG spark_worker_web_ui=8081
ARG spark_version

WORKDIR /home/spark/spark-${spark_version}-bin-hadoop3

EXPOSE ${spark_worker_web_ui}
CMD bin/spark-class org.apache.spark.deploy.worker.Worker spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> logs/spark-worker.out