from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, LongType, TimestampType, DoubleType, StringType, IntegerType



spark = (
    SparkSession
    .builder 
    .master("local[*]")
    .appName("test")
    .getOrCreate()
    )

df_green = spark.read.parquet('spark/data/input/green/*/*')
df_green = df_green.withColumnRenamed('lpep_pickup_datetime', "pickup_datetime") \
                    .withColumnRenamed('lpep_dropoff_datetime', "dropoff_datetime")
df_green.printSchema()

df_yellow = spark.read.parquet('spark/data/input/yellow/*/*')
df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', "pickup_datetime") \
                    .withColumnRenamed('tpep_dropoff_datetime', "dropoff_datetime")
df_yellow.printSchema()

# select common columns
common_columns = []
yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)

df_green_com = df_green.select(common_columns).withColumn('service_type', F.lit('green'))
df_yellow_com = df_yellow.select(common_columns).withColumn('service_type', F.lit('yellow'))
data = df_green_com.unionAll(df_yellow_com)

data.groupBy('service_type').count().show()

"""TODO
- 다운받은 데이터를 조작하여 원하는 부분만 GCS로 저장

- 각 타입 별 사람들이 가장 많이 방문하는 곳 찾기 (DOlocationID)
- trip miles 비교
- tip, tolls 비교
- pick up, drop off 시간대
- green -> payment type
"""