from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from pprint import pprint
from re import findall
from typing import List, Dict, Generator, TypeAlias, Any
import argparse
import json
import sqlite3
import copy

Post: TypeAlias = Dict[str, int | str]
Posts: TypeAlias = Dict[int, Post]


class Converter:

    def __init__(self, markup: str | bytes) -> None:
        self.threads: Posts = {}
        self.soup = BeautifulSoup(markup, 'html.parser')

    def __elements_to_object(self) -> Posts:
        posts = self.soup.find_all('div', class_="post")

        for post in posts:
            metas = post.find_all("div", class_="meta")

            for meta in metas:

                number = meta \
                    .find("span", class_="number") \
                    .get_text(strip=True)

                name = meta \
                    .find("span", class_="name") \
                    .get_text(' ', strip=True)

                date = meta \
                    .find("span", class_="date") \
                    .get_text(strip=True)

                # slicing uid with "ID:"; e.g. "ID:TKRPJpAI0" -> "TKRPJpAI0"
                uid = meta \
                    .find("span", class_="uid") \
                    .get_text(strip=True)[3:]

            message = post \
                .find("span", class_="escaped") \
                .get_text('\n', strip=True)

            # 1000 レス目以降はただの広告なのでいらない
            if 1000 < int(number):
                break

            thread_datetime_extracted = findall(r'\d+', date)

            # add timezone information (Asia/Tokyo)
            thread_datetime = datetime \
                .strptime(
                    '%s/%s/%s %s:%s:%s.%s' % tuple(thread_datetime_extracted),
                    '%Y/%m/%d %H:%M:%S.%f') \
                .astimezone(timezone(timedelta(hours=9))) \
                .isoformat()

            self.threads.update({
                number: {
                    "number": int(number),
                    "name": name,
                    "date": thread_datetime,
                    "uid": uid,
                    "message": message
                }
            })

        return self

    def convert(self) -> Posts:
        self.__elements_to_object()
        return self.threads

    def json(self, **kwargs) -> str:
        """
        標準出力用のJSONをダンプする
        """
        self.__elements_to_object()
        return json.dumps(self.threads, **kwargs)


class Database:

    def __init__(self):
        pass

    def connect_database(self):
        con = sqlite3.connect(":memory:")
        self.cur = con.cursor()

        return self

    def create_tables(self):

        # テーブルを作成する
        self.cur.execute(
            """
            CREATE TABLE messages (
                bbskey INTEGER,
                bbs TEXT,
                title TEXT,
                number INTEGER, 
                name TEXT, 
                date TEXT, 
                uid TEXT, 
                message TEXT
            )
            """)
        return self

    def insert_records(self, threads):
        insert = copy.copy(threads)

        for i, x in enumerate(threads):
            insert[x].update({
                "bbskey": 1685975035,
                "bbs": "liveuranus",
                "title": "なんJNVA部★219",
            }),
        # pprint(threads)

        self.cur.executemany(
            """
            INSERT INTO messages 
            VALUES (
                :bbskey, 
                :bbs, 
                :title, 
                :number, 
                :name, 
                :date, 
                :uid, 
                :message
                )
            """,
            insert.values())

        return self

    def test(self):
        # pprint(cur.execute("SELECT name FROM sqlite_master WHERE TYPE='table'").fetchall())
        # pprint(cur.execute("SELECT * FROM sqlite_master").fetchall())
        pprint(self.cur.execute("SELECT * FROM messages").fetchall())
        return


def main():
    parser = argparse.ArgumentParser(
        prog='threadconv', description='Convert a thread in HTML into JSON')
    parser.add_argument('filename', help="Path to an HTML file",)
    # /home/iigau/dev/JNAI-Archives/html/なんJNVA部★148.html
    args = parser.parse_args()

    with open(args.filename, encoding="utf-8") as fp:
        thread = Converter(fp)
        threads = thread.convert()
        # threads = thread.json(ensure_ascii=False, indent=4)

    db = Database()
    db.connect_database() \
        .create_tables() \
        .insert_records(threads) \
        .test()

    # print(threads)


if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:
        pass
