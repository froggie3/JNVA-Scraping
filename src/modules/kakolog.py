
import itertools
import json
import logging
import sys
from time import sleep
from types import SimpleNamespace
import requests
from modules.classes import Request
from modules.types import Response, Threads


class KakologThreadsRequest(Request):
    ENDPOINT = "https://kakolog.jp/ajax/ajax_search.v16.cgi"

    def __init__(self, query: str) -> None:
        super().__init__()
        self.response_text: str = ""
        self.search_query: str = query

    def __get(self, **kwargs) -> Response | None:
        try:
            r = requests.get(**kwargs)
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            logging.exception("ページの取得中にエラーが発生しました")
            return None
        return r

    def set_response(self, s):
        self.response_text = s

    def get(self, **kwargs) -> None:
        if (r := self.__get(**kwargs)) is not None:
            self.set_response(r.text)

    def prepare_query(self, query: str, page: int) -> dict:
        return {"q": query, "p": page}

    def request_page_of(self, page=0) -> str | None:
        q = self.prepare_query(self.search_query, page)
        self._update_header(self.ENDPOINT)
        logging.debug("http header updated: %s", self.headers)
        logging.debug(q)
        self.get(url=self.ENDPOINT, headers={}, params=q, timeout=5)
        if self.response_text is not None:
            return self.response_text
        return None


class KakologThreadsIndexer:
    def __init__(self, query) -> None:
        self.__threads: Threads = {}
        self.search_query: str = query

    def extract_status(self, a: list):
        """抽出されたスレの目次のデータを辞書に格納する"""
        _k = map(lambda x: SimpleNamespace(**x), a)
        k = [f"https://{v.server}/test/read.cgi/{v.bbs}/{v.bbskey}/" for v in _k]
        s = dict(zip(k, a))
        # the key 'title' always has extra spaces, so removing it
        for _, v in s.items():
            v["title"] = v["title"].rstrip()
        return s

    def append_threads(self, x):
        self.__threads.update(x)

    def __request_api_many(self):
        """過去ログの API のURLに向かって繰り返しリクエストする"""
        rq = KakologThreadsRequest(self.search_query)
        for page in itertools.count():
            r = rq.request_page_of(page)
            if r is not None:
                j = json.loads(r)
                # 'list': 各スレの見出しのデータで、最後のページで空になる
                if not (lst := j.get("list")):
                    logging.info("the last page reached")
                    break
                logging.info(
                    "successfully downloaded indices of threads (on page %d)", page
                )

                if lst is not None:
                    chunk = self.extract_status(lst)
                    self.append_threads(chunk)
                sleep(1)
            else:
                # 例外処理
                if len(self.__threads) < 1:
                    sys.exit(1)
                # 一つでも取得したスレッドがあるなら次の処理へ
                break
        return self

    def out(self):
        return self.__threads

    def get_index(self) -> Threads:
        self.__request_api_many()
        logging.info("インデックスの取得が終了しました")
        return self.out()
