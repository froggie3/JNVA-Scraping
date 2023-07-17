#!/usr/bin/env python3
import argparse
import json
# import os
import re
import sqlite3
import sys
import traceback
from datetime import datetime, timedelta, timezone
from itertools import count
from multiprocessing import Pool, cpu_count
from re import findall
from textwrap import dedent
from time import sleep
from typing import Any, Dict, Generator, List, Tuple, TypeAlias

import requests
from bs4 import BeautifulSoup

from database_helper import Database
from modules.color import Color as c

Response: TypeAlias = requests.models.Response
Thread: TypeAlias = Dict[str, int | str]
Threads: TypeAlias = Dict[str, Thread]
ThreadsChunk: TypeAlias = List[Dict[str, str]]
Post: TypeAlias = Dict[str, int | str]
Posts: TypeAlias = Dict[int, Post]


class ThreadsIndexer:

    def __init__(self, query: str) -> None:
        self.searchquery = query
        self.threads: Threads = {}

    def __request_api(self):
        """
        過去ログの API のURLに向かって繰り返しリクエストする
        """
        api_url = "https://kakolog.jp/ajax/ajax_search.v16.cgi"

        # ページ送り / 次のページへ遷移
        for page in count():
            # スレッドのインデックスのデータが格納されている list[] を取得する
            # 最後のページでは [] が返されるので、代わりに空のオブジェクトを返す
            try:
                response = requests.get(api_url, headers={}, params={
                    'q': self.searchquery, 'p': page,
                    # "custom_date": '', "d": '', "o": '', "resnum": '', "bbs": '', "custom_resnum": '',
                    # "custom_resnum_dirup": '', "star": '',
                }, timeout=5)

                # サーバーが200以外を返したときの処理
                response.raise_for_status()

            except requests.exceptions.HTTPError as error:
                print(error)
                sys.exit(1)

            else:
                # 最後のページに達した (list[] が空になった)
                if not response.json().get('list'):
                    print(f"{c.BLUE}最後のページ{c.RESET}")
                    break

                print(f"ダウンロード完了 ({page + 1} ページ目)")

                for status in response.json().get('list'):

                    # 抽出されたデータを辞書に格納する
                    self.threads.update({
                        f"https://{status.get('server')}/test/read.cgi/{status.get('bbs')}/{status.get('bbskey')}/": {  # type: ignore
                            "ikioi": status.get('ikioi'),
                            "bbskey": status.get('bbskey'),
                            "created": status.get('created'),
                            "site": status.get('site'),
                            "is_live": status.get('is_live'),
                            "bbs": status.get('bbs'),
                            "is_update": status.get('is_update'),
                            "updated": status.get('updated'),
                            "id": status.get('id'),
                            "server": status.get('server'),
                            "title": status.get('title')
                            .rstrip(),  # type: ignore
                            "resnum": status.get('resnum')
                        }
                    })

            sleep(args.sleep)

        return self

    def __message(self):

        if self.threads:
            print(f"{c.BLUE}インデックスの取得に成功しました{c.RESET}")
        else:
            print(f"{c.RED}インデックスの取得に失敗しました{c.RESET}")

        return self

    def get_index(self) -> Threads:
        self.__request_api()
        self.__message()

        return self.threads


class Converter:

    def __init__(self, markup: str | bytes) -> None:
        """
        BeautifulSoupのインスタンスを作る
        """
        self.threads: Posts = {}
        self.soup = BeautifulSoup(markup, 'html.parser')

    def __elements_to_object(self):
        """
        HTMLをパースして辞書の形に整える
        """
        post_elements = self.soup.find_all('article')
        number = name = date = uid = message = ""
        exceeded_or_ronin = 1000

        for element in post_elements:
            metas = element.find_all("details", class_="post-header")

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

            message = element \
                .find("section", class_="post-content") \
                .get_text('\n', strip=True)

            if exceeded_or_ronin < int(number):
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

    def __add_bbskey(self, bbs_key: int):
        """
        データベースの正規化のために bbs_key を付与する
        """
        for x in self.threads:
            self.threads[x].update({"bbskey": bbs_key})
        return self

    def convert(self, bbs_key: int) -> Posts:
        self.__elements_to_object()
        self.__add_bbskey(bbs_key)
        return self.threads

    def json(self, **kwargs: Any) -> str:
        """
        標準出力用のJSONをダンプする
        """
        self.__elements_to_object()
        return json.dumps(self.threads, **kwargs)


class ThreadsDownloader:

    def generate_response(self, threads: List[str]) \
            -> Generator[str | None, None, None]:
        """
        条件に応じて、__fetch_thread() を回す
        """
        for url in threads:

            thread = self.__fetch_thread(url)

            if thread:
                # Replace "Shift_JIS" with "UTF-8" in meta tag
                yield thread.replace('charset=Shift_JIS', 'charset="UTF-8"')
                sleep(args.sleep)
            else:
                yield thread  # None

    def __fetch_thread(self, url: str) -> str | None:
        """
        スレッドが返ってくるまでダウンロードを試行
        """

        for _ in range(args.max_retry):

            # Header に host を追加
            matched = re.search(
                r"(?:https?://)((?:[\w-]+(?:\.[\w-]+){1,}))",
                url.strip())

            host = matched.group(1) if matched else url.strip()

            try:
                response = requests.get(url, headers={
                    "Alt-Used": host,
                    "Host": host,
                    "User-Agent": "Mozilla/5.0"
                }, timeout=10)

                # サーバーが200以外を返したときの処理
                response.raise_for_status()

            except requests.exceptions.HTTPError as error:
                print(error)
                response = None

            if response is not None:

                # Gone. が返ってきたら False を返す
                if "Gone.\n" not in response.text:
                    return response.text

                print(f"{c.BG_RED}Received invalid response. Retrying...{c.RESET}")

            sleep(args.sleep)

        return None


class ConverterDB(Database):

    def insert_messages(self, threads: Posts):
        """
        レスをテーブルに挿入する
        """
        self.cursor.executemany("""
        INSERT OR IGNORE INTO messages
        VALUES (
            :bbskey, :number,
            :name, :date,
            :uid, :message
        )
        """, threads.values())
        return self

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
                """, (index.get('server'),
                      index.get('bbs'),
                      index.get('bbskey'),
                      index.get('title'),
                      index.get('resnum'),
                      index.get('created'),
                      index.get('updated'),
                      index.get('is_live'),
                      index.get('resnum'),
                      index.get('updated'),
                      index.get('is_live')))

        return self

    def update_raw_data(self, bbs_key: str, markup: str):
        """
        生のHTMLデータをインデックスに挿入する
        """
        self.cursor.execute("""
        UPDATE
            thread_indexes
        SET
            raw_text = :text
        WHERE
            bbskey = :bbskey
        """, {
            "bbskey": bbs_key,
            "text": markup
        })
        return self

    def fetch_only_resumable(self) -> List[Tuple[str, int, str, str]]:
        self.cursor.execute("SELECT * FROM difference")
        return self.cursor.fetchall()

    def fetch_all_available(self) -> List[Tuple[str, int, str, str]]:
        self.cursor.execute("""
        SELECT
            server,
            bbs,
            bbskey,
            title
        FROM
            thread_indexes
        """)
        return self.cursor.fetchall()

    def fetch_raw_data(self, bbs_key: int):
        self.cursor.execute("""
        SELECT
            raw_text
        FROM
            thread_indexes
        WHERE
            bbskey = ?
        """, (bbs_key,))
        return self.cursor

    def fetch_bbs_key(self):
        self.cursor.execute("SELECT bbskey FROM difference")
        return self.cursor


class DownloadError(Exception):
    pass


def convert_parallel(bbs_key: int, markup_text: str):
    thread = Converter(markup_text)
    threads = thread.convert(bbs_key)
    print(f"Thread conversion {c.GREEN}{str(bbs_key)}{c.RESET} OK")
    return threads


def database_not_found(error: Exception):

    for arg in error.args:
        if arg.find("no such table"):
            print(traceback.format_exc())
            print(dedent(
                f"""
                {c.RED,}エラーが発生したで！{c.RESET}

                "database_helper.py を実行してテーブルを作成するんや
                """))
    sys.exit(1)


def cores_calculate(x: int) -> int:
    """1 <= x <= 最大コア数に制限"""
    return 1 if (x <= 0) else cpu_count() if (x > cpu_count()) else x


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='スレッドをデータベースに保存する',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        '-q',
        '--query',
        default="なんJNVA部",
        help="検索クエリを指定 (既定:「%(default)s」)",
        metavar="query",
        required=False,
    )

    parser.add_argument(
        '-s',
        '--skip',
        action='store_true',
        default=False,
        help="インデックスを取得しない (変換処理だけしたいとき便利です)",
        required=False,
    )

    parser.add_argument(
        '-c',
        '--cores',
        default=cpu_count() // 2,
        help="指定した数のコアを変換処理に使う (このコンピュータでの既定: %(default)s)",
        required=False,
        type=int,
    )

    parser.add_argument(
        '-t',
        '--sleep',
        default=5,
        help="どれくらいの間隔で落とすか (既定: %(default)s秒)",
        metavar="secs",
        required=False,
        type=int,
    )

    parser.add_argument(
        '-r',
        '--max-retry',
        default=5,
        help="HTTPエラー発生時などの最大再試行回数 (既定: %(default)s回)",
        metavar="tries",
        required=False,
        type=int,
    )

    parser.add_argument(
        '--force-archive',
        action='store_true',
        default=False,
        help="",
        required=False,
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

        print(f"{c.BLUE}インデックスを取得します{c.RESET}")

        try:
            dict_retrieved = indx.get_index()
            dict_retrieved = {x: dict_retrieved[x]
                              for x in reversed(dict_retrieved)}

        except KeyboardInterrupt as e:
            print(e)
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

    dldr = ThreadsDownloader()

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
        print(f"{c.GREEN}スレッドを取得します{c.RESET}")

    #
    # ダウンロード開始
    #

    generator = dldr.generate_response(
        [f"https://{x[0]}/test/read.cgi/{x[1]}/{x[2]}/" for x in posts])

    for post in posts:

        bbskey: str = post[2]  # bbs key in JSON
        title: str = post[3]

        print(f"{title} ... ", end="")

        try:
            text = next(generator)  # raw text from HTML

            if text is None:
                raise DownloadError(
                    "The page was failed to be retrieved and skipped due to an error while the process of downloading.")

            db.update_raw_data(bbskey, text)

        except (KeyboardInterrupt, DownloadError) as e:
            db.commit().close()

            print(f"{c.BG_RED}{''.join(e.args)}{c.RESET}\n")

            print(
                f"{c.GREEN}Saved the progress of what you have downloaded.{c.RESET}")

            sys.exit(1)

        # 正常終了
        else:
            db.commit()

        print(f"{c.GREEN}[OK]{c.RESET}")

    if posts:
        print(f"{c.GREEN}スレッドの取得に成功しました{c.RESET}")

    #
    # HTML の変換処理
    #

    bbskeys = []

    try:
        # bbskey を取得する
        bbskeys = [key[0] for key in db.fetch_bbs_key()]

    except sqlite3.OperationalError as e:
        database_not_found(e)

    if bbskeys:
        print(f"{c.GREEN}変換処理を開始します{c.RESET}")

    process_args: List[Tuple[int, str]] = []

    # bbskey をキーに raw_text からデータをとってくる
    for bbskey in bbskeys:
        for raw in db.fetch_raw_data(int(bbskey)):
            process_args.append((int(bbskey), *raw))

    # マルチプロセスで処理
    with Pool(processes=cores_calculate(args.cores)) as pool:
        thread_processed = pool.starmap(convert_parallel, process_args)

    if thread_processed:

        for thread_in_processed in thread_processed:
            db.insert_messages(thread_in_processed)

        print("アーカイブ中 ...")

        db.commit().close()

        print(f"{c.GREEN}全てのスレッドを保存しました{c.RESET}")

    else:
        print("更新はありません")
