from bs4 import BeautifulSoup
from database_helper import Database, create_database
from datetime import datetime, timezone, timedelta
from itertools import count
from modules.color import Color as c
from multiprocessing import cpu_count, Pool
from pprint import pprint
from re import findall
from requests.exceptions import HTTPError
from time import sleep
from typing import Any, Dict, Generator, List, Tuple, TypeAlias
from urllib.parse import quote
import argparse
import json
import re
import requests
import sqlite3


Response: TypeAlias = requests.models.Response
Thread: TypeAlias = Dict[str, int | str]
Threads: TypeAlias = Dict[str, Thread]
Post: TypeAlias = Dict[str, int | str]
Posts: TypeAlias = Dict[int, Post]


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

        while True:
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


class ThreadsDownloader:

    def generate_response(self, threads: List[str]) -> Generator[str | None, None, None]:
        """
        条件に応じて、fetch_thread() を回す
        """
        for url in threads:

            thread = self.fetch_thread(url)

            if thread:
                # Replace "Shift_JIS" with "UTF-8" in meta tag
                yield thread.replace('charset=Shift_JIS', 'charset="UTF-8"')
                sleep(2)
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
                print(c.BG_RED
                      + "Received invalid response. Retrying..."
                      + c.RESET)
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

        while (has_valid_response == False):
            response = fetch(url)
            if response is not None:
                has_valid_response = has_response(response.text)
                if not has_valid_response:
                    sleep(3)
                else:
                    return response.text
            else:
                sleep(3)
        return None


class dotdict(dict):  # type: ignore
    """
    dot.notation access to dictionary attributes
    """
    __getattr__ = dict.get  # type: ignore
    __setattr__ = dict.__setitem__  # type: ignore
    __delattr__ = dict.__delitem__  # type: ignore


class ConverterDB(Database):

    def insert_records(self, threads: Posts):
        self.cur.executemany("""
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

    def test(self):
        pprint(self.cur.execute("SELECT * FROM messages").fetchall())
        return


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


class DownloaderDB(Database):

    def update(self, bbskey: str, markup: str):
        self.cur.execute("""
        UPDATE thread_indexes  SET raw_text = :text  WHERE bbskey = :bbskey
        """, {
            "bbskey": bbskey,
            "text": markup
        })
        return self


class DownloadError(Exception):
    pass


def convert_parallel(bbskey: int, text: str):
    thread = Converter(text)
    threads = thread.convert(bbskey)
    print(f"Thread conversion - "
          + c.GREEN
          + str(bbskey)
          + c.RESET
          + " OK")
    return threads


if __name__ == "__main__":
    """
    スレッドとレス（ポスト）は区別する
    """

    parser = argparse.ArgumentParser(
        prog='threaddl',
        description='Download multiple threads in HTML based on a list'
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

    args = parser.parse_args()

    #
    # インデックスの取得
    #

    db = IndexerDB()
    db.connect_database()

    if args.skip:
        dict_retrieved = {}

    else:
        indx = ThreadsIndexer(args.query, vars(args))
        dict_retrieved = indx.create_index()
        dict_retrieved = {x: dict_retrieved[x]
                          for x in reversed(dict_retrieved)}

    # リトライ用のループ
    for _ in range(2):

        try:
            # データベースに更新分だけ追加
            db.insert_records(dict_retrieved) \
                .commit() \
                .close()
            break   # 正常終了でリトライループから抜ける

        except Exception as e:
            # pprint(e.args)
            if "no such table" in "".join(e.args):
                print(sqlite3.OperationalError)
                print("テーブルが見つかりませんでした。データーベースを作成します。")
                create_database()
                print(c.GREEN + "再試行します" + c.RESET)

    #
    # スレッドのダウンロード
    #

    dldr = ThreadsDownloader()

    db = DownloaderDB()
    db.connect_database()

    # ダウンロードURLを生成
    posts = db.cur \
        .execute("""
        SELECT
            server,
            bbs,
            bbskey,
            title
        FROM
            thread_indexes
        WHERE
            raw_text IS ?
        """, ('',)) \
        .fetchall()  # [(0, 1, 2), ]

    # ダウンロード
    generator = dldr.generate_response(
        ["https://%s/test/read.cgi/%s/%s/" % (x[0], x[1], x[2])
         for x in posts]
    )

    for post in posts:

        bbskey: str = post[2]  # bbs key in JSON
        title: str = post[3]

        print(f"{c.GREEN + title + c.RESET} ... ", end="")

        try:
            text = next(generator)  # raw text from HTML
            if text is None:
                raise DownloadError(
                    "The page was failed to be retrieved and skipped "
                    + "due to an error while the process of downloading.\n"
                )
            db.update(bbskey, text)

        except (KeyboardInterrupt, DownloadError) as e:
            db \
                .commit() \
                .close()
            print(c.BG_RED
                  + "".join(e.args)
                  + c.RESET)
            print(c.GREEN
                  + "Saved the progress of what you have downloaded so far to database."
                  + c.RESET)
            exit(1)

        # 正常終了
        else:
            db.commit()

        print(c.GREEN + f"[OK]" + c.RESET)

    db.close()

    #
    # 変換処理
    #

    db = ConverterDB()
    db.connect_database()

    # bbskey を取得する
    bbskeys = [key[0] for key in
               db.cur.execute("SELECT bbskey from difference")]

    process_args: List[Tuple[int, str]] = []
    # bbskey をキーに raw_text からデータをとってくる
    for bbskey in bbskeys:
        for j in db.cur.execute(
                """
                SELECT
                    raw_text
                from
                    thread_indexes
                WHERE
                    bbskey = ?
                """, (int(bbskey),)):
            process_args.append((int(bbskey), *j))

    # マルチプロセスで処理
    with Pool(processes=cpu_count() // 2) as pool:
        thread_processed = pool.starmap(convert_parallel, process_args)

    for thread_in_processed in thread_processed:
        db.insert_records(thread_in_processed)

    print("Archiving ...")

    db.commit() \
      .close()

    print(c.GREEN + "All threads saved." + c.RESET)
