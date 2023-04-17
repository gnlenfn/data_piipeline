docker build \
    --build-arg spark_version="3.3.1" \
    -f spark-base/spark-base.Dockerfile \
    -t spark-base .

docker build \
    -f spark-master/master.Dockerfile \
    -t spark-master .

docker build \
    -f spark-worker/worker.Dockerfile \
    -t spark-worker .
