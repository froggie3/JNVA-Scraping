from Color import Color as c
from os import path, makedirs
from requests.exceptions import HTTPError
from time import sleep
from typing import List, Generator, Dict, TypeAlias
import argparse
import codecs
import json
import re
import requests

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
            sleep(5)

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


def start_task(path_to_JSON: str, save_directory: str):
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

    generator = thread.generate_response(threads)

    for url in threads:
        title = threads[url]
        text = next(generator)
        if text is None:
            print(c.BG_RED
                  + "The page was failed to be retrieved and skipped "
                  + "due to an error while the process of downloading.\n"
                  + "Please relieve the download list in JSON (or something)"
                  + "will be kept intact."
                  + c.RESET)
            return
        print(text)
        # save_to(text, path.join(save_directory, f"{title}.html"))


def save_to(self, text, file_path) -> None:
    """
    取得したテキストを指定のパス名で保存する
    """
    with codecs.open(file_path, "w", "utf-8") as fp:
        fp.write(text)
    print(c.BG_WHITE
          + f"Exported to {file_path}"
          + c.RESET)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog='threaddl',
        description='Download multiple threads in HTML based on a list')
    parser.add_argument('jsonpath', metavar="JSON",
                        help="Path to JSON containing thread URLs and titles",)
    parser.add_argument('destination', metavar="DESTINATION",
                        help="The directory for thread to be downloaded",)
    try:
        args = parser.parse_args()
        start_task(args.jsonpath, args.destination)

    except KeyboardInterrupt:
        pass
