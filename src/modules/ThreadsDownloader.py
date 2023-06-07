import codecs
import json
import os
import shutil
import re
from colr import color
from modules import Timer
from modules import HTTPRequest


class ThreadsDownloader:
    has_verbose = False

    def __init__(self, args) -> None:
        self.args = args
        self.json_path = args.json[0]
        self.old_json_path = self.json_path.replace(".json", ".old.json")

        if args.verbose:
            self.has_verbose = args.verbose

        if args.outdir:
            self.out_dir = args.outdir[0]

    def main(self):
        print(color("[INFO] Looking for thread indexes...", fore="blue"))
        timer = Timer(self.args)
        dir = self.out_dir
        thread_list = self.__jsonLoader(self.json_path)

        if os.path.exists(path=self.old_json_path):
            self.delete_latest_archive()
        else:
            self.duplicate_latest_json(self.json_path, self.old_json_path)

        print(color("[INFO] Start downloading archives...", fore="blue"))

        for thread in thread_list:
            title, url = thread.values()
            os.makedirs(name=dir, exist_ok=True)
            export_path = os.path.join(dir, f"{title}.html")

            if os.path.exists(path=export_path):
                if self.has_verbose:
                    print(f"    Skipped saving to {export_path}")
                continue

            for _ in range(99):
                req = HTTPRequest()
                r = req.fetch(url=url)
                body = r.text
                # print(body)
                if not re.findall("Gone.\n", body):
                    break
                else:
                    # Gone. が返ってきたら再試行する
                    print("    Received invalid response. Retrying...")
                    timer.sleep()

            with codecs.open(filename=export_path, mode="w", encoding="utf-8") as fp:
                fp.write(body.replace("charset=Shift_JIS", "charset=\"UTF-8\""))

            if os.path.exists(path=export_path):
                print(color(f"    Exported to {export_path}", fore="blue"))

            timer.sleep()

    def verify(self):
        print(color("[INFO] Downloading finished", fore="blue"))

    def duplicate_latest_json(self, src: str, dest: str) -> None:
        print(color(
            f"[WARN] {dest} was not found. Making a copy of JSON file...", fore="yellow"))
        try:
            shutil.copyfile(src, dest)
        except shutil.SameFileError:
            print("Same filename was specified both in source and destination")

    def find_latest_archive_name(self) -> str | None:
        list = self.__jsonLoader(self.old_json_path) or None
        # 事前に中身をチェックしてから処理を開始する
        if list is not None:
            name = list[0]
            if "thread_title" in name:
                name = name["thread_title"]
                renamed = os.path.join(self.out_dir, f"{name}.html")
                return renamed
        return None

    def delete_latest_archive(self) -> None:
        # 最新のスレッドは基本埋まっていない
        p = self.find_latest_archive_name() or None
        if p is not None:
            if os.path.exists(p):
                os.remove(p)
        else:
            print(
                "Failed to retrieve the latest thread name whereas previous archive files found")

    def __jsonLoader(self, path) -> dict:
        try:
            with open(file=path, mode="r", encoding="utf-8") as fp:
                content = json.loads(fp.read())
        except FileNotFoundError:
            print(color(f"{path} was not found!", fore="red"))
        else:
            print(f"    Found {path}")
            return content
