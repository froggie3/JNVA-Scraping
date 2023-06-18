from bs4 import BeautifulSoup
from color import Color as c
from datetime import datetime, timezone, timedelta
from multiprocessing import cpu_count, Pool
from pprint import pprint
from re import findall
from typing import List, Dict, Tuple, TypeAlias, Any
import argparse
import json
import sqlite3

Post: TypeAlias = Dict[str, int | str]
Posts: TypeAlias = Dict[int, Post]


class Converter:

    def __init__(self, markup: str | bytes) -> None:
        self.threads: Posts = {}
        self.soup = BeautifulSoup(markup, 'html.parser')

    def __elements_to_object(self):
        posts = self.soup.find_all('article')
        number = name = date = uid = message = ""

        for post in posts:
            metas = post.find_all("details", class_="post-header")

            for meta in metas:

                number = meta \
                    .find("span", class_="postid") \
                    .get_text(strip=True)

                name = meta \
                    .find("span", class_="postusername") \
                    .get_text(' ', strip=True)

                date = meta \
                    .find("span", class_="date") \
                    .get_text(strip=True)

                # slicing uid with "ID:"; e.g. "ID:TKRPJpAI0" -> "TKRPJpAI0"
                uid = meta \
                    .find("span", class_="uid") \
                    .get_text(strip=True)[3:]

            message = post \
                .find("section", class_="post-content") \
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
                int(number): {
                    "number": int(number),
                    "name": name,
                    "date": thread_datetime,
                    "uid": uid,
                    "message": message
                }
            })

        return self

    def __add_bbskey(self, bbskey: int):
        for x in self.threads:
            self.threads[x].update({"bbskey": bbskey})
        return self

    def convert(self, bbskey: int) -> Posts:
        self.__elements_to_object() \
            .__add_bbskey(bbskey)
        return self.threads

    def json(self, **kwargs: Any) -> str:
        """
        標準出力用のJSONをダンプする
        """
        self.__elements_to_object()
        return json.dumps(self.threads, **kwargs)


class Database:

    def connect_database(self):
        self.con = sqlite3.connect("src/modules/db.db")
        self.cur = self.con.cursor()
        return self

    def create_tables(self):
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            bbskey INTEGER,
            number INTEGER, 
            name TEXT, 
            date TEXT, 
            uid TEXT, 
            message TEXT,
            UNIQUE (bbskey, number)
        )""")
        return self

    def insert_records(self, threads: Posts):
        self.cur.executemany("""
        INSERT INTO messages 
        VALUES (
            :bbskey, 
            :number, 
            :name, 
            :date, 
            :uid, 
            :message
        )""", threads.values())
        return self

    def rollback(self):
        self.con.rollback()

    def commit(self):
        self.con.commit()
        return self

    def close(self):
        self.con.close()
        return

    def test(self):
        pprint(self.cur.execute("SELECT * FROM messages").fetchall())
        return


def convert_parallel(bbskey: int, text: str):
    thread = Converter(text)
    threads = thread.convert(bbskey)
    print(c.GREEN
          + f"Thread conversion for {bbskey}: finished!"
          + c.RESET)
    return threads


# def convert_single(bbskey: int, text: str):
#     for x in raw_texts:
#         thread = Converter(x)
#         threads = thread.convert()
#         print(len(threads))


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            prog='threadconv', description='Convert a thread in HTML into JSON')
        args = parser.parse_args()

        db = Database()
        db.connect_database() \
            .create_tables()

        # bbskey を取得する
        bbskeys = [key[0] for key in
                   db.cur.execute("SELECT bbskey from thread_indexes")]

        process_args: List[Tuple[int, str]] = []
        # bbskey をキーに raw_text からデータをとってくる
        for bbskey in bbskeys:
            for j in db.cur.execute(
                    "SELECT raw_text from thread_indexes WHERE bbskey = ?", (int(bbskey),)):
                process_args.append((bbskey, *j))

        # マルチプロセスで処理
        with Pool(processes=cpu_count() // 2) as pool:
            threads = pool.starmap(convert_parallel, process_args)

        for thread in threads:
            db.insert_records(thread)

        # pprint(db.test())

        print("archiving ...")

        db.commit() \
          .close()

        print(c.GREEN + "All threads saved." + c.RESET)

    except KeyboardInterrupt:
        pass
