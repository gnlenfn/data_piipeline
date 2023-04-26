import pytchat #실시간 댓글 크롤링 
import pafy #유튜브 정보 
import pandas as pd 
import json
import time
from dotenv import load_dotenv
import os
import logging 
from logging import handlers
import asyncio

from google.cloud import storage

# logging
CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
CURRENT_FILE = os.path.basename(__file__)
LOG_FILENAME = f"log-{CURRENT_FILE[:-3]}"
LOG_DIR = f"{CURRENT_PATH}/logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

load_dotenv(f"{CURRENT_PATH}/env/.env")

logger = logging.getLogger()
logger.setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s %(levelname)s:%(message)s")

file_log_handler = handlers.TimedRotatingFileHandler(
    filename=f"{CURRENT_PATH}/logs/{LOG_FILENAME}", when='midnight', interval=1, encoding='utf-8'
    )
file_log_handler.suffix = ".log-%Y-%m-%d"
file_log_handler.setLevel(logging.INFO)
file_log_handler.setFormatter(formatter)

console_log_handler = logging.StreamHandler()
console_log_handler.setLevel(logging.INFO)
console_log_handler.setFormatter(formatter)

logger.addHandler(file_log_handler)
logger.addHandler(console_log_handler)

# environment varialbes
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
    df = pd.DataFrame(columns=['id', 'datetime', 'name', 'message', 'author_channel', 'is_chat_moderator'])

    while chat.is_alive():
        data = chat.get()
        items = data.items

        for c in items:
            data.tick()
            j = json.loads(c.json())
            new_row = {
                "id": j['id'],
                "datetime": j['datetime'],
                "message": j['message'],
                "name": j['author']['name'],
                "author_channel": j["author"]['channelId'],
                "is_chat_moderator": j['author']['isChatModerator']
            }
            df.loc[len(df)] = new_row

        if len(df) >= 5: 
            asyncio.run(save_df_to_gcs(df))
            df = pd.DataFrame(columns=['id', 'datetime', 'name', 'message', 'author_channel', 'is_chat_moderator'])

    if len(df):
        asyncio.run(save_df_to_gcs(df))


async def save_df_to_gcs(df: pd.DataFrame) -> None:
    df = df.astype(str)
    df.to_parquet(file_path)

    # upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"streaming_chat/{FILE_NAME}_{int(time.time())}.parquet")
    blob.upload_from_filename(f"./{FILE_NAME}.parquet")

    logger.warning(f"{len(df)} length parquet uploaded!")


if __name__ == "__main__":
    scrape_chats()
