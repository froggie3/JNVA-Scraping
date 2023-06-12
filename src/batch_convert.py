from pprint import pprint
import argparse
import json
import random
import sqlite3
import threading
import time

semaphore = threading.BoundedSemaphore(value=3)


def limited_task(task_id):
    with semaphore:
        print(f"Task {task_id} started.")
        time.sleep(random.randrange(1, 1000) * 0.001)
        print(f"Task {task_id} finished.")


def start_task():
    threads = [threading.Thread(target=limited_task, args=(x,))
               for x in range(100)]
    # print(threads)
    for thread in threads:
        thread.start()
    # for thread in threads:
    #     thread.join()
    print("All threads finished.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog='threadconv',
        description='Convert a thread in HTML into JSON')

    parser.add_argument(
        'filename',
        help="Path to an HTML file",
    )

    args = parser.parse_args()

    try:
        with open(args.filename, encoding="utf-8") as fp:
            json = json.load(fp)
        con = sqlite3.connect(":memory:")
        cur = con.cursor()

        # pprint(cur.execute("SELECT name FROM sqlite_master WHERE TYPE='table'").fetchall())
        # pprint(cur.execute("SELECT * FROM sqlite_master").fetchall())
        pprint(cur.execute("SELECT * FROM bruh_table").fetchall())

        # print(json)

    except KeyboardInterrupt:
        pass


# /home/iigau/dev/JNAI-Archives/html/なんJNVA部★148.html
