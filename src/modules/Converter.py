from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from pprint import pprint
from re import findall
from typing import List, Dict, Generator
import argparse
import json

parser = argparse.ArgumentParser(
    prog='threadconv',
    description='Convert a thread in HTML into JSON')

parser.add_argument(
    'filename',
    help="Path to an HTML file",
)

args = parser.parse_args()

with open(args.filename, encoding="utf-8") as fp:
    soup = BeautifulSoup(fp, 'html.parser')

threads = {}

posts = soup.find_all('div', class_="post")

for post in posts:
    metas = post.find_all("div", class_="meta")

    for meta in metas:
        number: int = int(meta.find("span", class_="number")
                          .get_text(strip=True))
        name: str = meta.find("span", class_="name") \
            .get_text(' ', strip=True)
        date: str = meta.find("span", class_="date") \
            .get_text(strip=True)
        # slicing uid with "ID:"; e.g. "ID:TKRPJpAI0" -> "TKRPJpAI0"
        uid: str = meta.find("span", class_="uid") \
            .get_text(strip=True)[3:]

    message: str = post.find("span", class_="escaped") \
        .get_text('\n', strip=True)

    if number <= 1000:
        thread_datetime_extracted = findall(r'\d+', date)

        # add timezone information (Asia/Tokyo)
        thread_datetime = datetime.strptime(
            '%s/%s/%s %s:%s:%s.%s' % tuple(thread_datetime_extracted),
            '%Y/%m/%d %H:%M:%S.%f')\
            .astimezone(timezone(timedelta(hours=9))) \
            .isoformat()

        threads.update({
            number: {
                "number": number,
                "name": name,
                "date": thread_datetime,
                "uid": uid,
                "message": message
            }
        })
    else:
        break

print(json.dumps(threads, ensure_ascii=False, indent=4))
