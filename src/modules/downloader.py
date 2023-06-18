from color import Color as c
from database import Database
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

Response: TypeAlias = requests.models.Response
Thread: TypeAlias = Dict[str, Dict[str, int | str]]
Threads: TypeAlias = Dict[str, Thread]


class ThreadsDownloader:

    def generate_response(self, threads: Dict[str, str]) -> Generator[str | None, None, None]:
        """
        条件に応じて、fetch_thread() を回す
        """
        skip = False
        for url in threads:
            if skip:  # すでに重複したレコードがあるとき
                continue
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


class DownloaderDB(Database):

    def update(self, bbskey: str, markup: str):
        self.cur.execute("""
        UPDATE thread_indexes  SET raw_text = :text  WHERE bbskey = :bbskey
        """, {
            "bbskey": bbskey,
            "text": markup
        })
        return self

    def test(self):
        # pprint(cur.execute("SELECT name FROM sqlite_master WHERE TYPE='table'").fetchall())
        # pprint(cur.execute("SELECT * FROM sqlite_master").fetchall())
        pprint(self.cur
               .execute("SELECT * FROM thread_indexes")
               .fetchall())
        return self
    
    def fetchall(self):
        self.cur.fetchall()
        return self


class DownloadError(Exception):
    pass


def create_download_list() -> List:

    def obsolete_prepare() -> List:
        # タスクリスト
        with open(path_to_JSON, encoding="utf-8") as fp:
            threads = json.load(fp)
        return threads

    def prepare() -> List:
        print(c.BG_WHITE
              + "Looking for thread indexes..."
              + c.RESET)
        

        pass

    def exists_bbskey() -> bool:
        """データベースにすでにレコードがあるかを調べる"""
        
        db.cur.execute("SELECT thread_indexes WHERE bbskey = ?", (bbskey, ))
        

        exists_bbskey = db.cur.fetchone() is not None
        return exists_bbskey

    def download():
        print(c.BG_WHITE
              + "Started downloading archives..."
              + c.RESET)
        pass

    pass


def start_task(path_to_JSON: str) -> None:

    thread = ThreadsDownloader()
    db = DownloaderDB()
    db.connect_database()

    # ダウンロードURLを生成
    db.cur.execute("SELECT server, bbs, bbskey from thread_indexes WHERE bbskey = ?", (bbskey, ))

    threads = 

    # https://fate.5ch.net/test/read.cgi/liveuranus/1686204895/
    # (\w+)/(\d+)/$

    generator = thread.generate_response(threads)

    for i in threads:
        title: str = threads[i]['title']
        bbskey: str = threads[i]['bbskey']  # bbs key in JSON

        print(c.BLUE
              + f"Downloading a webpage for {title} ... "
                + c.RESET, end="")

        try:
            if True:
                pass
            else:
                print('exists!')

            # text = next(generator)  # raw text from HTML

            # if text is None:
            #     raise DownloadError(
            #         "The page was failed to be retrieved and skipped "
            #         + "due to an error while the process of downloading.\n"
            #     )
            pass

        except (KeyboardInterrupt, DownloadError) as e:
            print(*[c.BG_RED, e, c.RESET])
            print(c.GREEN
                  + "Saving the progress of what you have downloaded so far to database."
                  + c.RESET)
            # db.commit()
            db.close()
            return

        else:
            db.update(bbskey, text)

        print(c.GREEN + f"OK" + c.RESET)
        # print(text)
        # save_to(text, path.join(save_directory, f"{title}.html"))

    # db.commit()
    db.close()


def save_to(text: str, file_path: str) -> None:
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
