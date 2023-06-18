from color import Color as c
from database import Database
from itertools import count
from pprint import pprint
from time import sleep
from typing import List, Dict, Generator, TypeAlias
from urllib.parse import quote
import argparse
import requests

Thread: TypeAlias = Dict[str, int | str]
Threads: TypeAlias = Dict[str, Thread]


class ThreadsIndexer:

    def __init__(self, query: str, args: Dict[str, str | bool]) -> None:
        self.searchquery = query

    def threads_array_compose(self, data: List[Dict[str, str]]) -> Threads:
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
            print(c.BLUE
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
                print("Reached the end of pages")
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


class IndexerDB (Database):

    def insert_records(self, data: Threads):
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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog='threadindexer',
        description='Convert a thread in HTML into JSON')

    parser.add_argument(
        '-q', '--query', metavar="QUERY",
        help="What word would you like to look for? (default: %(default)s)",
        required=False, default="なんJNVA部"
    )

    parser.add_argument(
        '-s', '--skip', action='store_true',
        help="skip the process of downloading", default=False, required=False
    )

    try:
        args = parser.parse_args()

        db = IndexerDB()
        db.connect_database()

        if args.skip:
            dict_retrieved = {}

        else:
            thread = ThreadsIndexer(args.query, vars(args))
            dict_retrieved = thread.create_index()
            dict_retrieved = {x: dict_retrieved[x]
                              for x in reversed(dict_retrieved)}

        # データベースに更新分だけ追加
        db.insert_records(dict_retrieved) \
            .commit() \
            .close()

    except KeyboardInterrupt:
        pass
