from pprint import pprint
from requests import utils
from time import sleep
from typing import List, Dict, Generator
import itertools
import json
import requests
from Color import Color


class ThreadsIndexer:
    def __init__(self, query, args=None) -> None:
        self.searchquery = query
        self.verbose = args.verbose if args else True

    def threads_array_compose(self, data: List) -> List[Dict[str, dict[str, str]]]:
        return {f"https://{x['server'].rstrip()}/test/read.cgi/{x['bbs'].rstrip()}/{x['bbskey'].rstrip()}/": {
                "thread_title": x["title"].rstrip()
                } for x in data}

    def make_index(self) -> Dict[str, Dict[str, str]]:
        threads = {}
        generator = self.query_generator()

        while True:
            # 取得できるまで繰り返す
            data = self.download_one_thread(next(generator))
            if data:
                threads.update(**data)
                sleep(1)
            else:
                break

        if self.verbose:
            # pprint(threads, width=80)
            pass

        if threads:
            print(Color.BLUE +
                  "Succeeded to retrieve thread names and links" + Color.RESET)
            return threads
        else:
            print(Color.RED + "Failed to retrieve threads names and links" + Color.RESET)
            return {}

    def download_one_thread(self, url: str) -> List[str]:
        r = requests.get(url=url, headers={})

        if self.verbose:
            # pprint([url, r.headers], width=30)
            pass

        print(f"{Color.BG_BLUE if r.status_code == 200 else Color.BG_RED}{r.status_code}{Color.RESET}: {url}")

        if r.status_code == requests.codes.ok:
            if r.json()["list"]:
                # データを抽出して格納して次のページへ遷移
                return self.threads_array_compose(r.json()["list"])
            else:
                print(Color.BG_WHITE + "Reached the end of pages" + Color.RESET)
                return []
        else:
            # サーバーが200以外を返したときの処理
            pass
        return []

    def query_generator(self) -> Generator[str, None, None]:
        for i in itertools.count():
            yield f"https://kakolog.jp/ajax/ajax_search.v16.cgi?q={utils.quote(self.searchquery)}&custom_date=&d=&o=&resnum=&bbs=&custom_resnum=&custom_resnum_dir=up&p={i}&star="

    def to_string(self, data) -> str:
        dump = json.dumps(data, indent=4, ensure_ascii=True)
        return dump
