from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder 
    .appName 
    .getOrCreate()
    )

"""
spark dataframe 혹은 SQL로 찾을 내용
- 하루 동안 가장 채팅을 활발하게 친 사람 찾기 (parquet 하루 단위로 저장될 것)
- 가장 채팅이 활발한 시간대 찾기
- 키워드 순위를 내기는 쉽지 않을듯..?
"""