"""
- TCL Trip Record Data를 다운받아 -> os (wget), request, urllib
    - request 호출로 수동으로 파일쓰기: 인코딩 문제 가능성?
    - urllib.urlretreieve: python2의 레거시라 deprecated 가능성 있음..?
    - 그냥 직접 리눅스 wget호출로 (os, subprocess)
- parquet 파일을 직접 다운받아 GCS bucket에 업로드

이후 Spark에서는 parquet를 다운받아서 쓸지? 혹은 직접 읽어서 쓸지?
"""

import os
from google.cloud import storage
import logging

from config.default import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)s:%(message)s")
CURRENT_FILE = os.path.basename(__file__)

console_log_handler = logging.StreamHandler()
console_log_handler.setLevel(logging.INFO)
console_log_handler.setFormatter(formatter)

logger.addHandler(console_log_handler)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_KEY_PATH
BUCKET_NAME = "2023-de-zoomcamp"

def download_trip_data() -> None:
    os.system("cd data && mkdir yellow green fhv fhvhv")
    try:
        for type in ['yellow', 'green', 'fhv', 'fhvhv']:
            for year in [2020, 2021, 2022]:
                for month in range(1, 13):
                    os.system(f"wget {BASE_URL}/{type}_tripdata_{year}-{month:0>2}.parquet -P spark/data/{type}/{year}")

    except KeyboardInterrupt:
        raise KeyboardInterrupt


def parquet_to_gcs() -> None:
    for type in ['yellow', 'green', 'fhv', 'fhvhv']:
        for year in [2020, 2021, 2022]:
            for month in range(1, 13):
                # upload gcs
                storage_client = storage.Client()
                bucket = storage_client.bucket(BUCKET_NAME)
                blob = bucket.blob(f"data/{type}/{year}/{type}_treipdata_{year}-{month:0>2}.parquet")
                blob.upload_from_filename(f"spark/data/{type}/{year}/{type}_tripdata_{year}-{month:0>2}.parquet")
                logger.info(f"{type}_{year}-{month} uploaded!")


if __name__ == "__main__":
    download_trip_data()
    parquet_to_gcs()
