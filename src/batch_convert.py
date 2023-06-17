from pprint import pprint
import argparse
import json
import random
import sqlite3
import threading
import time
from modules.converter import Converter as conv
from modules.color import Color as c

semaphore = threading.BoundedSemaphore(value=5)


def task(task_id, text):
    with semaphore:
        print(f"Task {task_id} started.")
        thread = conv(text)
        threads = thread.convert()
        # time.sleep(random.randrange(1, 1000) * 0.001)
        print(c.BG_BLUE + f"Task {task_id} finished." + c.RESET)


def start_task():
    with open("/home/iigau/dev/JNAI-Archives/html/なんJNVA部★148.html",
              encoding="utf-8") as fp:
        text = fp.read()
    threads = [threading.Thread(target=task, args=(x, text))
               for x in range(10)]
    # print(threads)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("All threads finished.")


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(
    #     prog='threadconv',
    #     description='Convert a thread in HTML into JSON')

    # parser.add_argument(
    #     'filename',
    #     help="Path to an HTML file",
    # )

    # args = parser.parse_args()

    try:
        start_task()
        # con = sqlite3.connect(":memory:")
        # cur = con.cursor()
        # pprint(cur.execute("SELECT name FROM sqlite_master WHERE TYPE='table'").fetchall())
        # pprint(cur.execute("SELECT * FROM sqlite_master").fetchall())
        # pprint(cur.execute("SELECT * FROM bruh_table").fetchall())
        # print(json)

    except KeyboardInterrupt:
        pass


# /home/iigau/dev/JNAI-Archives/html/なんJNVA部★148.html
