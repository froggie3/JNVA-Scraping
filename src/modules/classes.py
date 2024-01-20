import json
import logging
import sys
import sqlite3
from datetime import datetime, timedelta, timezone
from re import findall
from typing import Any, List, Dict, Tuple
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from modules.types import Response, Posts, Threads
from modules.errors import BadContentError
from modules.argments import args
from modules.vars import JNVADB_PATH


class Database:
    def __init__(self):
        assert JNVADB_PATH is not None
        self.connect = sqlite3.connect(JNVADB_PATH)
        self.cursor = self.connect.cursor()

    def rollback(self):
        self.connect.rollback()

    def commit(self):
        self.connect.commit()
        return self

    def close(self):
        self.connect.close()
        return


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
            "UPDATE thread_indexes SET raw_text = :text WHERE bbskey = :bbskey", {
                "bbskey": bbs_key,
                "text": markup
            })
        return self

    def fetch_only_resumable(self) -> List[Tuple[str, int, str, str]]:
        self.cursor.execute("SELECT * FROM difference")
        return self.cursor.fetchall()

    def fetch_all_available(self) -> List[Tuple[str, int, str, str]]:
        self.cursor.execute("SELECT server, bbs, bbskey, title FROM thread_indexes")
        return self.cursor.fetchall()


class Request:
    def __init__(self) -> None:
        self.headers = {}

    def _update_header(self, url: str) -> None:
        r = urlparse(url)
        host = r.netloc
        self.headers = {"Alt-Used": host, "Host": host, "User-Agent": "Mozilla/5.0"}


class ThreadsIndexer:
    ENDPOINT = "https://find.5ch.net/search"

    def __init__(self, query: str) -> None:
        self.query = query

    def _prepare_query(self) -> Dict[str, str]:
        return {"q": self.query}

    def __get(self) -> Response:
        assert self.query is not None
        req = requests.request(
            "GET", self.ENDPOINT, params=self._prepare_query(), timeout=10
        )
        return req

    def extract(self, markup: str) -> Dict[str, Any] | None:
        def extract_serverinfo(s: str) -> Tuple[str, str]:
            r = urlparse(s)
            # return dict(zip(("bbs", "server",), (r.path[1:-1], r.netloc,)))
            return r.path[1:-1], r.netloc

        def extract_datetime(s: str) -> str:
            return datetime.strptime(s, "%Y年%m月%d日 %H:%M") \
                .astimezone(timezone(timedelta(hours=9))) \
                .isoformat()

        def extract_bbskey(s: str) -> str:
            r = urlparse(s)
            return r.path.split("/")[-1]

        def extract_ikioi(s: str) -> float:
            return float(s.replace("/日", ""))

        def extract_resnum(s: str) -> int:
            """ "Hello(New) (1002)のような文字列から'(1002)'を取り出す"""
            a = s.rsplit(" (")[-1][:-1]
            assert isinstance(a, str)
            return int(a)

        def extract_title(s: str) -> str:
            return "".join(s.rsplit(" (")[:-1])

        def extract_is_live(n: int) -> int:
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

    def get_index(self) -> Dict[str, Any] | None:
        markup = self.__get().text
        a = self.extract(markup)
        if a is not None:
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

    def json(self, **kwargs) -> str:
        """
        標準出力用のJSONをダンプする
        """
        self.__elements_to_object()
        return json.dumps(self.threads, **kwargs)


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

    def generate_response(self, threads: list):
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
