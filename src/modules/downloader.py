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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog='threaddl',
        description='Download multiple threads in HTML based on a list'
    )

    args = parser.parse_args()

    thread = ThreadsDownloader()

    db = DownloaderDB()

    db.connect_database()

    # ダウンロードURLを生成
    rows = db.cur.execute("SELECT * FROM difference")
    threads = rows.fetchall()

    # ダウンロード
    generator = thread.generate_response(
        ["https://%s/test/read.cgi/%s/%s/" % (x[0], x[1], x[2])
         for x in threads]
    )

    for i in threads:

        bbskey: str = threads[i][2]  # bbs key in JSON
        title: str = threads[i][3]

        print(c.BLUE
              + f"Downloading a webpage for {title} ... "
                + c.RESET, end="")

        try:
            text = next(generator)  # raw text from HTML
            if text is None:
                raise DownloadError(
                    "The page was failed to be retrieved and skipped "
                    + "due to an error while the process of downloading.\n"
                )

        except (KeyboardInterrupt, DownloadError) as e:
            print(*[c.BG_RED, e, c.RESET])
            print(c.GREEN
                  + "Saving the progress of what you have downloaded so far to database."
                  + c.RESET)
            db.commit()
            db.close()
            exit

        else:
            db.update(bbskey, text)

        print(c.GREEN + f"OK" + c.RESET)

    db.commit()
    db.close()
