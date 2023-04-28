import pytchat #실시간 댓글 크롤링 
import pafy #유튜브 정보 
import pandas as pd 
import json
from datetime import datetime, timedelta
import logging 
from logging import handlers
from apscheduler.schedulers.background import BackgroundScheduler
from google.cloud import storage

from config.default import *

logger = logging.getLogger()
logger.setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s %(levelname)s:%(message)s")

LOG_FILENAME = f"{os.path.dirname(os.path.realpath(__file__))[:-3]}"
file_log_handler = handlers.TimedRotatingFileHandler(
    filename=f"{BASE_PATH}/logs/{LOG_FILENAME}", when='midnight', interval=1, encoding='utf-8'
    )
file_log_handler.suffix = "log-%Y-%m-%d"
file_log_handler.setLevel(logging.INFO)
file_log_handler.setFormatter(formatter)

console_log_handler = logging.StreamHandler()
console_log_handler.setLevel(logging.INFO)
console_log_handler.setFormatter(formatter)

logger.addHandler(file_log_handler)
logger.addHandler(console_log_handler)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_KEY_PATH
pafy.set_api_key(YOUTUBE_API_KEY) 

video_id = 'FJfwehhzIhw'  # TODO: 달라지는 영상 ID 어떻게 추적?
BUCKET_NAME = "2023-de-zoomcamp"
FILE_NAME = "news_ytn_youtube"


def scrape_chats() -> None:
    chat = pytchat.create(video_id=video_id)
    df = pd.DataFrame(columns=['id', 'datetime', 'name', 'message', 'author_channel', 'is_chat_moderator'])
    
    logger.info("while loop started!")
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

        now = datetime.now().time()
        if now.hour == 15 and now.minute == 0:  # 자정 되면 지금까지 쌓인것 모두 저장 -> UTC 시간으로 생각해야함 (GCP 인스턴스 기준 -09:00)
            save_to_csv(df)
            df = pd.DataFrame(columns=['id', 'datetime', 'name', 'message', 'author_channel', 'is_chat_moderator'])
            continue

        if len(df) >= 100: 
            save_to_csv(df)
            df = pd.DataFrame(columns=['id', 'datetime', 'name', 'message', 'author_channel', 'is_chat_moderator'])

    if len(df):
        save_to_csv(df)


def save_to_csv(df: pd.DataFrame) -> None:
    if not os.path.exists(f"youtube/{FILE_NAME}.csv"):
        df.to_csv(f"youtube/{FILE_NAME}.csv", mode='w', encoding='utf-8')
    else:
        df.to_csv(f"youtube/{FILE_NAME}.csv", mode='a', encoding='utf-8', header=False)

    logger.info(f"saved {len(df)} csv file!")
    

def csv_to_gcs_parquet() -> None:
    df = pd.read_csv(f"youtube/{FILE_NAME}.csv")
    df.to_parquet(f"youtube/{FILE_NAME}.parquet")

    # upload gcs
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    tday = datetime.today().strftime('%Y-%m-%d')  # gcs로 저장 cron을 자정(KST) 지나서 하지만 UTC 시간은 아직 전날이므로 날짜변동 X
    blob = bucket.blob(f"streaming_chat/{FILE_NAME}_{tday}.parquet")
    blob.upload_from_filename(f"./{FILE_NAME}.parquet")

    logger.info(f"{len(df)} length dataframe saved!")

    if os.path.isfile(f"youtube{FILE_NAME}.csv"):
        os.remove(f"youtube/{FILE_NAME}.csv")
        os.remove(f"youtube/{FILE_NAME}.parquet")
        logger.info("csv and parquet file cleaned!")


if __name__ == "__main__":
    scrape_chats()

    schedule = BackgroundScheduler()
    schedule.start()
    schedule.add_job(csv_to_gcs_parquet, 'cron', hour=0, minute =30)  # 00:30에 csv를 parquet로
