import pytchat #실시간 댓글 크롤링 
import pafy #유튜브 정보 
import pandas as pd 
import json
import time
from dotenv import load_dotenv
import os
from logging import handlers
import logging 

from google.cloud import storage

load_dotenv("env/.env")

log_handler = handlers.TimedRotatingFileHandler(filename="logs/chat.log", when='midnight', interval=1, encoding='utf-8')
log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s:%(message)s"))
log_handler.suffix = "%Y-%m-%d"

logger = logging.getLogger()
logger.setLevel(logging.WARNING)
logger.addHandler(log_handler)

GCS_KEY_PATH = os.environ["GCS_KEY_PATH"]
BUCKET_NAME = "2023-de-zoomcamp"
FILE_NAME = "news_ytn_youtube"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_KEY_PATH

YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"] #gcp youtube data api 에서 api key 생성 
pafy.set_api_key(YOUTUBE_API_KEY) 

video_id = 'FJfwehhzIhw' # TODO: 달라지는 영상 ID 어떻게 추적?
file_path = './news_ytn_youtube.parquet'


def scrape_chats() -> None:
    chat = pytchat.create(video_id=video_id)
    df = pd.DataFrame()

    while chat.is_alive():

        data = chat.get()
        items = data.items

        for c in items:
            # print(f"{c.datetime} [{c.author.name}]- {c.message}")
            data.tick()
            j = json.loads(c.json())
            df = pd.concat([df, pd.json_normalize(j)], ignore_index=True)

        if len(df) == 10000: 
            save_df_to_gcs(df)
            df = pd.DataFrame()

    if len(df):
        save_df_to_gcs(df)


def save_df_to_gcs(df: pd.DataFrame) -> None:
    df = df.astype(str)
    df.to_parquet(file_path)

    # upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"streaming_chat/{FILE_NAME}_{time.time()}.parquet")
    blob.upload_from_filename(f"./{FILE_NAME}.parquet")

    logger.warning("parquet uploaded!")


if __name__ == "__main__":
    scrape_chats()
