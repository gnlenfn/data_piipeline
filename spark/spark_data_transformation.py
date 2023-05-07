"""TODO
- 다운받은 데이터를 조작하여 원하는 부분만 GCS로 저장
-> read from GCS directly

- 각 타입 별 사람들이 가장 많이 방문하는 곳 찾기 (DOlocationID)
- trip miles 비교
- tip, tolls 비교
- pick up, drop off 시간대
- green -> payment type
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, LongType, TimestampType, DoubleType, StringType, IntegerType


spark = (
    SparkSession
    .builder 
    .master("local[*]")
    .appName("test")
    .config('spark.jars', "spark/config/mysql-connector-j-8.0.32.jar")
    .getOrCreate()
    )

df_green = spark.read.parquet('spark/data/input/green/*/*')
df_green = df_green.withColumnRenamed('lpep_pickup_datetime', "pickup_datetime") \
                    .withColumnRenamed('lpep_dropoff_datetime', "dropoff_datetime")
# df_green.printSchema()

df_yellow = spark.read.parquet('spark/data/input/yellow/*/*')
df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', "pickup_datetime") \
                    .withColumnRenamed('tpep_dropoff_datetime', "dropoff_datetime")
# df_yellow.printSchema()

# select common columns
common_columns = []
yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)

df_green_com = df_green.select(common_columns).withColumn('service_type', F.lit('green'))
df_yellow_com = df_yellow.select(common_columns).withColumn('service_type', F.lit('yellow'))
data = df_green_com.unionAll(df_yellow_com)
data = data.filter(data.pickup_datetime >= '2020-01-01')  # 과거 데이터가 섞여있었음

# data.groupBy('service_type').count().show()
"""
맨하탄 북부에서만 영업하는 green taxi에 비해 
뉴욕 전역에서 서비스하는 yellow taxi의 건수가 10배 이상
"""

data.createOrReplaceTempView("trip_data")  # sql 사용을 위한 테이블 등록
monthly_agg = spark.sql("""
SELECT
    DATE_FORMAT(date_trunc('month', pickup_datetime), '%y-%M') AS month,
    service_type,

    ROUND(SUM(fare_amount), 3) AS monthly_fare,
    ROUND(SUM(tip_amount), 3) AS monthly_tip,
    ROUND(SUM(tolls_amount), 3) AS monthly_tolls,
    ROUND(SUM(mta_tax), 3) AS monthly_mta_tax,
    ROUND(SUM(congestion_surcharge), 3) AS monthly_congestion_charge,

    ROUND(AVG(passenger_count), 3) AS avg_monthly_passengers,
    ROUND(AVG(trip_distance), 3) AS avg_monthly_trip_distance
FROM
    trip_data
WHERE
    service_type = 'yellow'
GROUP BY
    1, 2
ORDER BY
    1 ASC
""")

monthly_agg.show()

monthly_agg.write \
    .format("jdbc") \
    .mode('overwrite') \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/pipeline?useSSL=false&allowPublicKeyRetrieval=true") \
    .option("dbtable", "monthly_revenue") \
    .option("user", "root") \
    .option("password", "root") \
    .save()
