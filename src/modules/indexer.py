from color import Color as c
from itertools import count
from itertools import count
from os import path
from pprint import pprint
from time import sleep
from typing import List, Dict, Generator, TypeAlias
from urllib.parse import quote
import argparse
import json
import requests
import sqlite3

Thread: TypeAlias = Dict[str, Dict[str, int | str]]
Threads: TypeAlias = Dict[str, Thread]


class ThreadsIndexer:

    def __init__(self, query, args=None) -> None:
        self.searchquery = query
        self.verbose = args.verbose if args else True

    def threads_array_compose(self, data: List) -> Threads:
        # 現行スレはリストに追加しないようにする
        # 基準: "is_live" が 1 になっているか、 resnum < 1002 以下?
        return {
            "https://%s/test/read.cgi/%s/%s/" % (x['server'], x['bbs'], x['bbskey']): {
                "ikioi": x["ikioi"],
                "bbskey": x["bbskey"],
                "created": x["created"],
                "site": x["site"],
                "is_live": x["is_live"],
                "bbs": x["bbs"],
                "is_update": x["is_update"],
                "updated": x["updated"],
                "id": x["id"],
                "server": x["server"],
                "title": x["title"].rstrip(),
                "resnum": x["resnum"]
            } for x in data if int(x["is_live"]) == 0
        }

    def create_index(self) -> Threads:
        threads: Threads = {}
        generator = self.query_generator()

        for i in count():
            # 取得できるまで繰り返す
            data = self.download_one_thread(next(generator))
            if data:
                threads.update(**data)
                sleep(2)
            else:
                break

        if threads:
            print(c.BG_BLUE
                  + "Succeeded to retrieve thread names and links"
                  + c.RESET)
            return threads
        else:
            print(c.RED
                  + "Failed to retrieve threads names and links"
                  + c.RESET)
            return {}

    def download_one_thread(self, url: str) -> Threads:
        r = requests.get(url=url, headers={})

        print("%s%s%s: %s" % (
            c.BG_BLUE if r.status_code == 200 else c.BG_RED,
            r.status_code, c.RESET, url))

        if r.status_code == requests.codes.ok:

            if r.json()["list"]:
                # データを抽出して格納して次のページへ遷移
                return self.threads_array_compose(r.json()["list"])
            else:
                print(c.BG_WHITE
                      + "Reached the end of pages"
                      + c.RESET)
                return {}
        else:
            # サーバーが200以外を返したときの処理
            pass
        return {}

    def query_generator(self) -> Generator[str, None, None]:

        for i in count():
            yield f"https://kakolog.jp/ajax/ajax_search.v16.cgi" \
                + f"?q={quote(self.searchquery)}" \
                + "&custom_date=" \
                + "&d=" \
                + "&o=" \
                + "&resnum=" \
                + "&bbs=" \
                + "&custom_resnum=" \
                + "&custom_resnum_dir=up" \
                + f"&p={i}" \
                + "&star="


class Database:

    def __init__(self):
        pass

    def connect_database(self):
        # self.con = sqlite3.connect(":memory:")
        self.con = sqlite3.connect("db.db")
        self.cur = self.con.cursor()

        return self

    def create_table(self):
        # self.cur.execute("""
        # CREATE TABLE IF NOT EXISTS thread_indexes (
        #     ikioi INTEGER,
        #     bbskey INTEGER,
        #     created TEXT,
        #     site TEXT,
        #     is_live TEXT,
        #     bbs TEXT,
        #     is_update TEXT,
        #     updated TEXT,
        #     id TEXT TEXT,
        #     server TEXT,
        #     title TEXT,
        #     resnum INTEGER,
        #     UNIQUE (
        #         bbs, 
        #         bbskey
        #         )
        #     )""")
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS thread_indexes (
            server TEXT,
            bbs TEXT,
            bbskey INTEGER,
            title TEXT,
            resnum INTEGER,
            created TEXT,
            updated TEXT,
            raw_text TEXT,
            UNIQUE (
                bbs, 
                bbskey
                )
            )""")

        return self

    def insert_records(self, data: Threads):
        # self.cur.executemany("""
        # INSERT OR IGNORE INTO thread_indexes
        # VALUES (
        #     :ikioi,
        #     :bbskey,
        #     :created,
        #     :site,
        #     :is_live,
        #     :bbs,
        #     :is_update,
        #     :updated,
        #     :id,
        #     :server,
        #     :title,
        #     :resnum
        #     )""", data.values())
        self.cur.executemany("""
        INSERT OR IGNORE INTO thread_indexes
        VALUES (
            :server,
            :bbs,
            :bbskey,
            :title,
            :resnum,
            :created,
            :updated,
            ''
            )""", data.values())
        
        return self

    def commit(self):
        self.con.commit()
        return self

    def close(self):
        self.con.close()
        return

    def test(self) -> None:
        # pprint(cur.execute("SELECT name FROM sqlite_master WHERE TYPE='table'").fetchall())
        # pprint(cur.execute("SELECT * FROM sqlite_master").fetchall())
        pprint(self.cur
               .execute("SELECT * FROM thread_indexes")
               .fetchall())
        return self


parser = argparse.ArgumentParser(
    prog='threadindexer',
    description='Convert a thread in HTML into JSON')

parser.add_argument(
    '-v', '--verbose',
    help="Outputs redundant information",
    default=False
)

parser.add_argument(
    'jsonpath', metavar="JSONPATH",
    help="Path to the JSON file",
)

parser.add_argument(
    '-q', '--query', metavar="QUERY",
    help="What word would you like to look for? (default: %(default)s)",
    required=False, default="なんJNVA部"
)

parser.add_argument(
    '-s', '--skip', action='store_true',
    help="skip the process of downloading", default=False, required=False
)

parser.add_argument(
    '--json-out', action='store_true',
    help="enables JSON output", default=False, required=False
)


def main():
    args = parser.parse_args()
    print(args)

    if args.skip:
        dict_retrieved = {}
    else:
        thread = ThreadsIndexer(args.query, args)
        dict_retrieved = thread.create_index()
        dict_retrieved = {x: dict_retrieved[x]
                            for x in reversed(dict_retrieved)}

    # もしあるなら過去のJSONを読み込む
    if path.exists(args.jsonpath):
        with open(args.jsonpath, encoding="utf-8") as fp:
            past = json.load(fp)
            past = {x: past[x] for x in reversed(past)}

        # 過去のJSONと比較して更新分だけ追加
        dict_retrieved.update(dict_diff(dict_retrieved, past))

    if args.json_out:
        # JSONにシリアライズして書き込み
        dump = json.dumps(dict_retrieved, ensure_ascii=False, indent=4)
        with open(args.jsonpath, "w", encoding="utf-8") as fp:
            fp.write(dump)
    else:
        # データベース
        db = Database()
        db.connect_database() \
            .create_table() \
            .insert_records(dict_retrieved) \
            .commit() \
            .test() \
            .close()

if __name__ == "__main__":

    def dict_diff(dict: dict, dicts: dict):
        return {x: dicts[x] for x in dicts if x not in dict}

    try:
        main()

    except KeyboardInterrupt:
        pass
