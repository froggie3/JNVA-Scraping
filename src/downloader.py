#!/usr/bin/env python3

# from collections import namedtuple
import itertools
import json
import logging
import sqlite3
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import datetime, timedelta, timezone
from pprint import pprint
from re import findall
from time import sleep
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple, TypeAlias
from urllib.parse import urlparse
from contextlib import closing

import requests
from bs4 import BeautifulSoup

from dotenv import load_dotenv
from database_helper import Database

# from textwrap import dedent
# from typing import TypedDict, Generator
import os
# import re


Response: TypeAlias = requests.models.Response
Thread: TypeAlias = Dict[str, int | str]
Threads: TypeAlias = Dict[str, Thread]
ThreadsChunk: TypeAlias = List[Dict[str, str]]
Post: TypeAlias = Dict[str, int | str]
Posts: TypeAlias = Dict[int, Post]

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.DEBUG)


class Request:
    def __init__(self) -> None:
        self.headers = {}

    def _update_header(self, url) -> None:
        r = urlparse(url)
        host = r.netloc
        self.headers = {"Alt-Used": host, "Host": host, "User-Agent": "Mozilla/5.0"}


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


class ThreadsIndexer:
    ENDPOINT = "https://find.5ch.net/search"

    def __init__(self, query=None) -> None:
        self.query = query

    def _prepare_query(self):
        return {"q": self.query}

    def __get(self) -> Response:
        assert self.query is not None
        req = requests.request(
            "GET", self.ENDPOINT, params=self._prepare_query(), timeout=10
        )
        return req

    def extract(self, markup: str):
        def extract_serverinfo(s: str) -> Tuple[str, str]:
            r = urlparse(s)
            # return dict(zip(("bbs", "server",), (r.path[1:-1], r.netloc,)))
            return r.path[1:-1], r.netloc

        def extract_datetime(s) -> str:

            return datetime.strptime(s, "%Y年%m月%d日 %H:%M") \
                .astimezone(timezone(timedelta(hours=9))) \
                .isoformat()

        def extract_bbskey(s) -> str:
            r = urlparse(s)
            return r.path.split("/")[-1]

        def extract_ikioi(s) -> float:
            return float(s.replace("/日", ""))

        def extract_resnum(s) -> int:
            """ "Hello(New) (1002)のような文字列から'(1002)'を取り出す"""
            a = s.rsplit(" (")[-1][:-1]
            assert isinstance(a, str)
            return int(a)

        def extract_title(s) -> str:
            return "".join(s.rsplit(" (")[:-1])

        def extract_is_live(n) -> int:
            return 0 if n > 1000 else 1

        soup = BeautifulSoup(markup, "html.parser")
        r = soup.find("div", "list")
        if r is not None:
            r_url = [e.get("href") for e in r.find_all("a", "list_line_link")]
            r_information_above = [
                e.get_text() for e in r.find_all("div", "list_line_link_title")
            ]
            r_title, r_resnum, r_server, r_bbs, r_bbskey, r_updated, r_ikioi, r_id, r_is_live = [
                [] for _ in range(9)
            ]

            for v in r_information_above:
                r_title.append(extract_title(v))
                resnum = extract_resnum(v)
                r_resnum.append(resnum)
                r_is_live.append(extract_is_live(resnum))

            r_information_below_elements = [
                [f for f in e if not isinstance(f, str)]
                for e in r.find_all("div", "list_line_info")
            ]
            # (server, server hostname), updated_datetime, ikioi
            r_information_below = [
                [
                    (
                        i.a.text,
                        i.a.get("href"),
                    ),
                    j.get_text(),
                    k.get_text(),
                ]
                for i, j, k in r_information_below_elements
            ]

            for seq, (_ser, date, ikioi) in enumerate(r_information_below):
                # print(ser)
                _, ser = _ser
                bbs, server = extract_serverinfo(ser)
                r_bbs.append(bbs)
                r_server.append(server)
                r_updated.append(extract_datetime(date))
                bbskey = extract_bbskey(r_url[seq])
                r_bbskey.append(bbskey)
                r_ikioi.append(extract_ikioi(ikioi))
                r_id.append(f"2ch/{bbs}/{bbskey}")

            a = {}
            for title, url, bbs, bbskey, server, updated, ikioi, _id, resnum, is_live in zip(
                r_title,
                r_url,
                r_bbs,
                r_bbskey,
                r_server,
                r_updated,
                r_ikioi,
                r_id,
                r_resnum,
                r_is_live,
            ):
                a.update(
                    {
                        url: {
                            "ikioi": ikioi,
                            "bbskey": bbskey,
                            "bbs": bbs,
                            "updated": updated,
                            "id": _id,
                            "server": server,
                            "title": title,
                            "resnum": resnum,
                            "is_live": is_live
                        }
                    }
                )
            # pprint(a)
            return a

    def get_index(self) -> Dict[str, dict]:
        markup = self.__get().text
        a = self.extract(markup)
        return a


class Converter:
    def __init__(self, bbs_key: int, markup: str | bytes) -> None:
        """
        BeautifulSoupのインスタンスを作る
        """
        self.threads: Posts = {}
        self.bbs_key = bbs_key
        self.soup = BeautifulSoup(markup, "html.parser")

    def __elements_to_object(self):
        """
        HTMLをパースして辞書の形に整える
        """
        post_elements = self.soup.find_all("article")
        number = name = date = uid = message = ""
        exceeded_or_ronin = 1000

        for element in post_elements:
            metas = element.find_all("details", class_="post-header")

            for meta in metas:
                number = meta.find("span", class_="postid").get_text(strip=True)
                name = meta.find("span", class_="postusername").get_text(
                    " ", strip=True
                )
                date = meta.find("span", class_="date").get_text(strip=True)
                # slicing uid with "ID:"; e.g. "ID:TKRPJpAI0" -> "TKRPJpAI0"
                uid = meta.find("span", class_="uid").get_text(strip=True)[3:]

            message = element.find("section", class_="post-content").get_text(
                "\n", strip=True
            )

            if exceeded_or_ronin < int(number):
                break

            # あぼーん
            if date == "NG":
                thread_datetime = ""
            else:
                thread_datetime_extracted = findall(r"\d+", date)
                assert len(thread_datetime_extracted) == 7
                # add timezone information (Asia/Tokyo)
                thread_datetime = (
                    datetime.strptime(
                        "%s/%s/%s %s:%s:%s.%s" % tuple(thread_datetime_extracted),
                        "%Y/%m/%d %H:%M:%S.%f",
                    )
                    .astimezone(timezone(timedelta(hours=9)))
                    .isoformat()
                )

            self.threads.update(
                {
                    int(number): {
                        "bbskey": self.bbs_key,
                        "number": int(number),
                        "name": name,
                        "date": thread_datetime,
                        "uid": uid,
                        "message": message,
                    }
                }
            )

        return self

    def convert(self) -> Posts:
        self.__elements_to_object()
        return self.threads

    def json(self, **kwargs: Any) -> str:
        """
        標準出力用のJSONをダンプする
        """
        self.__elements_to_object()
        return json.dumps(self.threads, **kwargs)


class BadContentError(Exception):
    pass


class ThreadsDownloader(Request):
    def fetch_thread(self, url: str) -> str | None:
        self._update_header(url)
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            if "Gone.\n" in response.text:
                raise BadContentError("found 'Gone.' in the response")
        except requests.exceptions.HTTPError:
            logging.exception("HTTP error occured")
            return None
        return response.text

    def __fetch_thread_try(self, url: str) -> str | None:
        """
        スレッドが返ってくるまでダウンロードを試行
        """
        for _ in range(args.max_retry):
            try:
                r = self.fetch_thread(url)
                return r
            except requests.exceptions.Timeout:
                logging.exception("error may be occured due to connectivity")
            except BadContentError:
                logging.exception("retrying...")
        sys.exit(1)

    def generate_response(self, threads):
        """
        条件に応じて、__fetch_thread() を回す
        """
        for server, bbs, bbskey, title in threads:
            fetch_url = f"https://{server}/test/read.cgi/{bbs}/{bbskey}/"
            thread = self.__fetch_thread_try(fetch_url)
            if thread:
                # Replace "Shift_JIS" with "UTF-8" in meta tag
                text = thread.replace("charset=Shift_JIS", 'charset="UTF-8"')
                yield (bbskey, title, text)
            else:
                yield None


class ConverterDB(Database):
    def insert_indexes(self, data: Threads):
        """
        インデックスをテーブルに挿入する
        """
        # raw_textは後から更新するので空っぽにしておく
        indexes = data.values()

        for index in indexes:
            self.cursor.execute(
                """
                INSERT INTO thread_indexes (
                    server, bbs, bbskey, title, resnum,
                    created, updated, raw_text, is_live)
                VALUES (?, ?, ?, ?, ?, ?, ?, '', ?)
                ON CONFLICT (bbs, bbskey)
                DO UPDATE SET
                    resnum  = ?,
                    updated = ?,
                    is_live = ?
                """,
                (
                    index.get("server"),
                    index.get("bbs"),
                    index.get("bbskey"),
                    index.get("title"),
                    index.get("resnum"),
                    index.get("created"),
                    index.get("updated"),
                    index.get("is_live"),
                    index.get("resnum"),
                    index.get("updated"),
                    index.get("is_live"),
                ),
            )

        return self

    def update_raw_data(self, bbs_key: str, markup: str):
        """
        生のHTMLデータをインデックスに挿入する
        """
        self.cursor.execute(
            """
        UPDATE
            thread_indexes
        SET
            raw_text = :text
        WHERE
            bbskey = :bbskey
        """,
            {"bbskey": bbs_key, "text": markup},
        )
        return self

    def fetch_only_resumable(self) -> List[Tuple[str, int, str, str]]:
        self.cursor.execute("SELECT * FROM difference")
        return self.cursor.fetchall()

    def fetch_all_available(self) -> List[Tuple[str, int, str, str]]:
        self.cursor.execute("SELECT server, bbs, bbskey, title FROM thread_indexes")
        return self.cursor.fetchall()


class DownloadError(Exception):
    pass


def database_not_found(error: Exception):
    for arg in error.args:
        if arg.find("no such table"):
            logging.exception(
                "Sqlite3 database not found. "
                "Please run 'database_helper.py' "
                "before the execution of this program"
            )
    sys.exit(1)


def cores_calculate(x: int) -> int:
    """1 <= x <= 最大コア数に制限"""
    return 1 if (x <= 0) else cpu_count() if (x > cpu_count()) else x


if __name__ == "__main__":

    parser = ArgumentParser(
        description="スレッドをデータベースに保存する", formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "-q",
        "--query",
        default="なんJNVA部",
        help="検索クエリを指定 (既定:「%(default)s」)",
        metavar="query",
        required=False,
    )
    parser.add_argument(
        "-s",
        "--skip",
        action="store_true",
        default=False,
        help="インデックスを取得しない (変換処理だけしたいとき便利です)",
        required=False,
    )
    parser.add_argument(
        "--convert-only",
        action="store_true",
        default=False,
        help="すでにダウンロードしたスレッドについて変換処理だけする",
        required=False,
    )
    parser.add_argument(
        "-t",
        "--sleep",
        default=5,
        help="どれくらいの間隔で落とすか (既定: %(default)s秒)",
        metavar="secs",
        required=False,
        type=int,
    )
    parser.add_argument(
        "-r",
        "--max-retry",
        default=5,
        help="HTTPエラー発生時などの最大再試行回数 (既定: %(default)s回)",
        metavar="tries",
        required=False,
        type=int,
    )
    parser.add_argument(
        "--force-archive", action="store_true", default=False, help="", required=False
    )
    args = parser.parse_args()

    #
    # インデックスの取得
    #

    db = ConverterDB()

    if args.skip:
        dict_retrieved = {}
    else:
        indx = ThreadsIndexer(args.query)
        logging.info("インデックスを取得します")

        try:
            dict_retrieved = indx.get_index()
            dict_retrieved = {x: dict_retrieved[x] for x in reversed(dict_retrieved)}
        except KeyboardInterrupt as e:
            logging.error(e)
            db.close()
            sys.exit(1)

    try:
        # インデックスから取得したデータをデータベースに追加し、必要な分だけ更新する
        db.insert_indexes(dict_retrieved).commit()
    except sqlite3.OperationalError as e:
        database_not_found(e)
        db.close()

    #
    # スレッドのダウンロード
    #

    downloader = ThreadsDownloader()

    #
    # ダウンロードするURLリストをデータベースから取得
    #

    if args.force_archive is True:
        # raw_text が埋まっていないものだけ
        posts = db.fetch_all_available()  # [(0, 1, 2), ]
    else:
        # 増減ダウンロード
        posts = db.fetch_only_resumable()

    if posts:
        logging.info("スレッドを取得します")

    #
    # ダウンロード開始
    #

    if not args.convert_only:
        for resp in downloader.generate_response(posts):
            if not resp:
                break

            bbskey, title, text = resp
            logging.info("%s ... ", title)

            try:
                if text is None:
                    raise DownloadError(
                        "The page was failed to be retrieved and skipped "
                        "due to an error while the process of downloading."
                    )
                db.update_raw_data(bbskey, text)
            except (KeyboardInterrupt, DownloadError) as e:
                db.commit().close()
                logging.error("".join(e.args))
                logging.error("Saved the progress of what you have downloaded.")
                sys.exit(1)
            else:
                db.commit()
            sleep(args.sleep)

            logging.info("Saving success")

        if posts:
            logging.info("スレッドの取得に成功しました")

    #
    # HTML の変換処理
    #

    with closing(sqlite3.connect(os.environ.get('JNVADB_PATH'))) as conn:
        with conn:
            keys = conn.execute("SELECT bbskey FROM difference").fetchall()

            if keys:
                logging.info("Started the conversion from raw HTML files")
                logging.debug("%s", keys)

                for k in keys:
                    bbs_key, title, text = conn.execute(
                        "SELECT bbskey, title, raw_text "
                        "FROM thread_indexes WHERE bbskey = ?", k) \
                        .fetchone()
                    logging.info("Thread conversion %s (%s) started", bbs_key, title)

                    thread = Converter(bbs_key=bbs_key, markup=text)
                    threads = thread.convert()
                    logging.info("Thread conversion %s (%s) success", bbs_key, title)

                    logging.info("Saving the archive of thread %s ...", bbs_key)
                    conn.executemany(
                        "INSERT OR IGNORE INTO messages "
                        "VALUES (:bbskey, :number, :name, :date, :uid, :message)",
                        threads.values(),
                    )
                    logging.info("Saving the archive of thread %s success", bbs_key)
                    conn.commit()

            logging.info("All threads was safely saved")
