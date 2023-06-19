import argparse
import json
# import os
import re
import traceback
from datetime import datetime, timedelta, timezone
from itertools import count
from multiprocessing import Pool, cpu_count
from re import findall
from time import sleep
from typing import Any, Dict, Generator, List, Tuple, TypeAlias
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError

from database_helper import Database
from modules.color import Color as c

Response: TypeAlias = requests.models.Response
Thread: TypeAlias = Dict[str, int | str]
Threads: TypeAlias = Dict[str, Thread]
ThreadsChunk: TypeAlias = List[Dict[str, str]]
Post: TypeAlias = Dict[str, int | str]
Posts: TypeAlias = Dict[int, Post]


class ThreadsIndexer:

    def __init__(self, query: str, args: Dict[str, str | bool]) -> None:
        self.searchquery = query
        self.threads: Threads = {}

    def __request_api(self):
        """
        過去ログの API のURLに向かって繰り返しリクエストする
        """

        def query_generator() -> Generator[tuple[int, str], None, None]:
            """
            ページ送り / 次のページへ遷移
            """
            for i in count():
                yield (i, "".join(
                    ("https://kakolog.jp/ajax/ajax_search.v16.cgi",
                     f"?q={quote(self.searchquery)}",
                     f"&p={i}"))
                )
                # yield f"https://kakolog.jp/ajax/ajax_search.v16.cgi" \
                #     + f"?q={quote(self.searchquery)}" \
                #     + "&custom_date=" \
                #     + "&d=" \
                #     + "&o=" \
                #     + "&resnum=" \
                #     + "&bbs=" \
                #     + "&custom_resnum=" \
                #     + "&custom_resnum_dir=up" \
                #     + f"&p={i}" \
                #     + "&star="

        generator = query_generator()

        while True:

            page, url = next(generator)

            """
            スレッドのインデックスのデータが格納されている list[] を取得する
            最後のページでは [] が返されるので、代わりに空のオブジェクトを返す
            """
            r = requests.get(url, headers={})

            # サーバーが200以外を返したときの処理
            if r.status_code != requests.codes.ok:
                continue

            # 最後のページに達した
            if not r.json().get('list'):
                print(f"{c.BLUE}最後のページ{c.RESET}")
                break

            print(f"ダウンロード完了 ({page + 1} ページ目)")

            threads_chunk: Threads = {}

            for status in r.json().get('list'):

                # 暫定的に現行スレは省く (条件 -> "is_live" = "1" or resnum < 1002)
                if int(status.get('is_live')) != 0:  # type: ignore
                    continue

                threads_chunk.update({
                    "https://%s/test/read.cgi/%s/%s/" %
                    (status.get('server'), status.get('bbs'), status.get('bbskey')): {  # type: ignore
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

            if threads_chunk:
                # 抽出されたデータを辞書に格納する
                self.threads.update(**threads_chunk)
                sleep(args.sleep)
            else:
                # list[] が空になった？
                break

        return self

    def __message(self):

        if self.threads:
            print(f"{c.BLUE}インデックスの取得に成功しました{c.RESET}")
        else:
            print(f"{c.RED}インデックスの取得に失敗しました{c.RESET}")

        return self

    def get_index(self) -> Threads:
        self.__request_api().__message()

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
        posts = self.soup.find_all('article')
        number = name = date = uid = message = ""
        EXCEEDED_OR_RONIN = 1000

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

            if EXCEEDED_OR_RONIN < int(number):
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
        """
        データーベースの正規化のために bbskey を付与する
        """
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


class ThreadsDownloader:

    def generate_response(self, threads: List[str]) \
            -> Generator[str | None, None, None]:
        """
        条件に応じて、fetch_thread() を回す
        """
        for url in threads:

            thread = self.fetch_thread(url)

            if thread:
                # Replace "Shift_JIS" with "UTF-8" in meta tag
                yield thread.replace('charset=Shift_JIS', 'charset="UTF-8"')
                sleep(args.sleep)
            else:
                yield thread  # None

    def fetch_thread(self, url: str) -> str | None:
        """
        スレッドが返ってくるまでダウンロードを試行
        """
        def has_response(text: str) -> bool:
            """
            Gone. が返ってきたら False を返す
            """
            if "Gone.\n" in text:
                print(f"{c.BG_RED}Received invalid response. Retrying...{c.RESET}")
                return False
            return True

        def fetch(url: str) -> Response | None:
            host = extract_hostname_from(url)
            try:
                response = requests.get(url, headers={
                    "Alt-Used": host,
                    "Host": host,
                    "User-Agent": "Mozilla/5.0"
                })
            except HTTPError as e:
                print(e)
                response = None
            return response

        def extract_hostname_from(url: str) -> str:
            matched = re.search(
                r"(?:https?://)((?:[\w-]+(?:\.[\w-]+){1,}))",
                url.strip())
            return matched.group(1) if matched else url.strip()

        has_valid_response = False

        while (has_valid_response is False):
            response = fetch(url)
            if response is not None:
                has_valid_response = has_response(response.text)
                if not has_valid_response:
                    sleep(args.sleep)
                else:
                    return response.text
            else:
                sleep(args.sleep)
        return None


class dotdict(dict):  # type: ignore
    """
    dot.notation access to dictionary attributes
    """
    __getattr__ = dict.get  # type: ignore
    __setattr__ = dict.__setitem__  # type: ignore
    __delattr__ = dict.__delitem__  # type: ignore


class ConverterDB(Database):

    def insert_messages(self, threads: Posts):
        """
        レスをテーブルに挿入する
        """
        self.cursor.executemany("""
        INSERT OR IGNORE INTO messages
        VALUES (
            :bbskey,
            :number,
            :name,
            :date,
            :uid,
            :message
        )""", threads.values())
        return self

    def insert_indexes(self, data: Threads):
        """
        インデックスをテーブルに挿入する
        """
        self.cursor.executemany("""
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

    def update_raw_data(self, bbskey: str, markup: str):
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
            "bbskey": bbskey,
            "text": markup
        })
        return self

    def fetch_only_resumable(self) -> List[Tuple[str, int, str, str]]:
        self.cursor.execute("""
        SELECT
            server,
            bbs,
            bbskey,
            title
        FROM
            thread_indexes
        WHERE
            raw_text IS ?
        """, (
            '',
        ))
        return self.cursor.fetchall()

    def fetch_raw_data(self, bbskey: int):
        self.cursor.execute("""
        SELECT
            raw_text
        FROM
            thread_indexes
        WHERE
            bbskey = ?
        """, (
            bbskey,
        ))
        return self.cursor

    def fetch_bbs_key(self):
        self.cursor.execute("""
        SELECT
            bbskey
        FROM
            difference
        """)
        return self.cursor


class DownloadError(Exception):
    pass


def convert_parallel(bbskey: int, text: str):
    thread = Converter(text)
    threads = thread.convert(bbskey)
    print(f"Thread conversion {c.GREEN}{str(bbskey)}{c.RESET} OK")
    return threads


def database_not_found(error: Exception):

    for arg in error.args:
        if arg.find("no such table"):
            print(traceback.format_exc())
            print(
                f"""
                {c.RED,}エラーが発生したで！{c.RESET}

                "database_helper.py を実行してテーブルを作成するんや
                """)
    exit(1)


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

    args = parser.parse_args()

    #
    # インデックスの取得
    #

    db = ConverterDB()
    db.connect_database()

    if args.skip:
        dict_retrieved = {}

    else:
        indx = ThreadsIndexer(args.query, vars(args))

        print(f"{c.BLUE}インデックスを取得します{c.RESET}")

        dict_retrieved = indx.get_index()
        dict_retrieved = {x: dict_retrieved[x]
                          for x in reversed(dict_retrieved)}

    try:
        # データベースに更新分だけ追加
        db.insert_indexes(dict_retrieved).commit()

    except Exception as e:
        database_not_found(e)
        db.close()

    #
    # スレッドのダウンロード
    #

    dldr = ThreadsDownloader()

    # ダウンロードURLを生成
    posts = db.fetch_only_resumable()  # [(0, 1, 2), ]

    if posts:
        print(f"{c.GREEN}スレッドを取得します{c.RESET}")

    # ダウンロード
    generator = dldr.generate_response(
        ["https://%s/test/read.cgi/%s/%s/" % (x[0], x[1], x[2])
         for x in posts]
    )

    for post in posts:

        bbskey: str = post[2]  # bbs key in JSON
        title: str = post[3]

        print(f"{c.GREEN}{title}{c.RESET} ... ", end="")

        try:
            text = next(generator)  # raw text from HTML
            if text is None:
                raise DownloadError(
                    "The page was failed to be retrieved and skipped due to an error while the process of downloading.\n"
                )
            db.update_raw_data(bbskey, text)

        except (KeyboardInterrupt, DownloadError) as e:
            db.commit().close()

            print(f"{c.BG_RED}{''.join(e.args)}{c.RESET}")

            print(
                f"{c.GREEN}Saved the progress of what you have downloaded.{c.RESET}")

            exit(1)

        # 正常終了
        else:
            db.commit()

        print(f"{c.GREEN}[OK]{c.RESET}")

    if posts:
        print(f"{c.GREEN}スレッドの取得に成功しました{c.RESET}")

    #
    # 変換処理
    #

    bbskeys = []

    try:
        # bbskey を取得する
        bbskeys = [key[0] for key in db.fetch_bbs_key()]

    except Exception as e:

        database_not_found(e)

    if bbskeys:
        print(f"{c.GREEN}変換処理を開始します{c.RESET}")

    process_args: List[Tuple[int, str]] = []
    # bbskey をキーに raw_text からデータをとってくる
    for bbskey in bbskeys:
        for j in db.fetch_raw_data(int(bbskey)):
            process_args.append((int(bbskey), *j))

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
