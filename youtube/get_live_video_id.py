import requests
from bs4 import BeautifulSoup

print("###")
channel = requests.get("https://www.youtube.com/@ytnnews24")
# channel = requests.get("https://www.google.co.kr/")
soup = BeautifulSoup(channel.text, "lxml")

a = soup.select('ytd-thumbnail')
# print(soup.select("ytd-channel-featured-content-renderer > div > ytd-video-renderer > div"))
for ans in a:
    print(ans)