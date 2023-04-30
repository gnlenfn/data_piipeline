docker build \
    --build-arg spark_version="3.3.2" \
    -f spark-base/spark-base.Dockerfile \
    -t spark-base .

docker build \
    --build-arg spark_version="3.3.2" \
    -f spark-master/master.Dockerfile \
    -t spark-master .

docker build \
    --build-arg spark_version="3.3.2" \
    -f spark-worker/worker.Dockerfile \
    -t spark-worker .

docker build \
    -f zeppelin/zeppelin.Dockerfile \
    -t spark-zeppelin .