from color import Color as c
from os import path, makedirs
from pprint import pprint
from requests.exceptions import HTTPError
from time import sleep
from typing import List, Generator, Dict, TypeAlias
import argparse
import codecs
import json
import re
import requests
import sqlite3

Response: TypeAlias = requests.models.Response


class ThreadsDownloader:

    def generate_response(self, threads: Dict[str, str]) -> Generator[str | None, None, None]:
        """
        fetch_thread()を回す
        """
        for url in threads:
            thread = self.fetch_thread(url)
            if thread is None:
                yield thread
                return
            # Replace "Shift_JIS" with "UTF-8" in meta tag
            yield thread.replace('charset=Shift_JIS', 'charset="UTF-8"')
            sleep(2)

    def fetch_thread(self, url) -> str | None:
        """
        スレッドが返ってくるまでダウンロードを試行
        """
        def has_response(text) -> bool:
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
                "(?:https?://)((?:[\w-]+(?:\.[\w-]+){1,}))",
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
        return None


class Database:

    def __init__(self):
        pass

    def connect_database(self):
        # self.con = sqlite3.connect(":memory:")
        self.con = sqlite3.connect("db.db")
        self.cur = self.con.cursor()

        return self

    def create_table(self):
        return self

    def update(self, bbskey: int, markup: str, ):
        self.cur.execute("""
        UPDATE thread_indexes  SET raw_text = :text  WHERE bbskey = :bbskey
        """, {
            "bbskey": bbskey,
            "text": markup
        })
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


def start_task(path_to_JSON: str) -> None:
    thread = ThreadsDownloader()

    print(c.BG_WHITE
          + "Looking for thread indexes..."
          + c.RESET)

    # タスクリスト
    with open(path_to_JSON, encoding="utf-8") as fp:
        threads: Dict[str, str] = json.load(fp)

    print(c.BG_WHITE
          + "Started downloading archives..."
          + c.RESET)

    db = Database()
    db.connect_database()

    # https://fate.5ch.net/test/read.cgi/liveuranus/1686204895/
    # (\w+)/(\d+)/$

    generator = thread.generate_response(threads)

    for i in threads:
        title = threads[i]['title']
        bbskey = threads[i]['bbskey']  # bbs key in JSON

        print(c.BLUE
              + f"Downloading a webpage for {title} ... "
                + c.RESET, end="")

        # データベースにすでにレコードがあるかを調べる

        # その場合、リクエストをしないという処理を書きたいが
        # 面倒くさい send() で何とかできるか？

        try:
            text = next(generator)  # raw text from HTML

            if text is None:
                print("")
                print(c.BG_RED
                      + "The page was failed to be retrieved and skipped "
                      + "due to an error while the process of downloading.\n"
                      + "Please relieve the download list in JSON (or something)"
                      + "will be kept intact."
                      + c.RESET)
                # ジェネレータ使ってるせいでリトライしづらくなってる
                # コストが重いジェネレータ
                return

        except KeyboardInterrupt as e:
            print(e)
            db.commit()
            db.close()
            return

        else:
            db.update(bbskey, text)

        print(c.GREEN + f"OK" + c.RESET)
        # print(text)
        # save_to(text, path.join(save_directory, f"{title}.html"))

    db.commit()
    db.close()


def save_to(self, text, file_path) -> None:
    """
    取得したテキストを指定のパス名で保存する
    """
    with codecs.open(file_path, "w", "utf-8") as fp:
        fp.write(text)
    print(c.BG_WHITE
          + f"Exported to {file_path}"
          + c.RESET)


def main() -> None:

    parser = argparse.ArgumentParser(
        prog='threaddl',
        description='Download multiple threads in HTML based on a list')
    parser.add_argument('jsonpath', metavar="JSON",
                        help="Path to JSON containing thread URLs and titles",)
    # parser.add_argument('destination', metavar="DESTINATION",
    #                     help="The directory for thread to be downloaded",)
    args = parser.parse_args()

    # start_task(args.jsonpath, args.destination)
    start_task(args.jsonpath)


if __name__ == "__main__":
    main()
