from Color import Color as c
from itertools import count
from os import path
from pprint import pprint
from time import sleep
from typing import List, Dict, Generator, TypeAlias
from urllib.parse import quote
import argparse
import json
import requests

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

        while True:
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

if __name__ == "__main__":

    def dict_diff(dict: dict, dicts: dict):
        return {x: dicts[x] for x in dicts if x not in dict}

    try:
        args = parser.parse_args()
        thread = ThreadsIndexer(args.query, args)
        dict_retrieved = thread.create_index()

        # もしあるなら過去のJSONを読み込む
        if path.exists(args.jsonpath):
            with open(args.jsonpath, encoding="utf-8") as fp:
                past = json.loads(fp.read())

            # 過去のJSONと比較して更新分だけ追加
            dict_retrieved.update(dict_diff(dict_retrieved, past))

        # JSONにシリアライズして書き込み
        dump = json.dumps(dict_retrieved, ensure_ascii=False, indent=4)
        with open(args.jsonpath, "w", encoding="utf-8") as fp:
            fp.write(dump)

    except KeyboardInterrupt:
        pass
