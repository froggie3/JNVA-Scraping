#!/usr/bin/env python3
import logging
import sqlite3
import sys
from time import sleep
from contextlib import closing
from modules.argments import args
from modules.vars import JNVADB_PATH
from modules.errors import DownloadError
from modules.classes import Converter, ThreadsDownloader, ThreadsIndexer, ConverterDB

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.DEBUG)


def convert() -> None:
    if JNVADB_PATH is not None:
        with closing(sqlite3.connect(JNVADB_PATH)) as conn:
            with conn:
                keys = conn.execute("SELECT bbskey FROM difference").fetchall()

                if keys:
                    logging.info("Started the conversion from raw HTML files")
                    logging.debug("%s", keys)

                    for k in keys:
                        bbs_key, title, text = conn.execute(
                            "SELECT bbskey, title, raw_text "
                            "FROM thread_indexes WHERE bbskey = ?", k) \
                            .fetchone()
                        logging.info("Thread conversion %s (%s) started", bbs_key, title)

                        thread = Converter(bbs_key=bbs_key, markup=text)
                        threads = thread.convert()
                        logging.info("Thread conversion %s (%s) success", bbs_key, title)

                        logging.info("Saving the archive of thread %s ...", bbs_key)
                        conn.executemany(
                            "INSERT OR IGNORE INTO messages "
                            "VALUES (:bbskey, :number, :name, :date, :uid, :message)",
                            threads.values(),
                        )
                        logging.info("Saving the archive of thread %s success", bbs_key)
                        conn.commit()

                logging.info("All threads was safely saved")


if __name__ == "__main__":
    # インデックスの取得
    db = ConverterDB()
    if args.skip:
        dict_retrieved = {}
    else:
        indx = ThreadsIndexer(args.query)
        logging.info("インデックスを取得します")
        dict_retrieved = indx.get_index()
        try:
            if dict_retrieved is None:
                raise ValueError
            dict_retrieved = {x: dict_retrieved[x] for x in reversed(dict_retrieved)}
        except KeyboardInterrupt as e:
            db.close()
            sys.exit(1)
        except ValueError:
            db.close()
            sys.exit(1)

    try:
        # インデックスから取得したデータをデータベースに追加し、必要な分だけ更新する
        db.insert_indexes(dict_retrieved).commit()
    except sqlite3.OperationalError as e:
        db.close()

    # スレッドのダウンロード
    downloader = ThreadsDownloader()
    # raw_text が埋まっていないものだけ、そうでなければ増減ダウンロード
    if args.force_archive is True:
        posts = db.fetch_all_available()  # [(0, 1, 2), ]
    else:
        posts = db.fetch_only_resumable()
    if posts:
        logging.info("スレッドを取得します")
    if not args.convert_only:
        for resp in downloader.generate_response(posts):
            if not resp:
                break
            bbskey, title, text = resp
            logging.info("%s ... ", title)
            try:
                if text is None:
                    raise DownloadError(
                        "The page was failed to be retrieved and skipped "
                        "due to an error while the process of downloading."
                    )
                db.update_raw_data(bbskey, text)
            except (KeyboardInterrupt, DownloadError) as e:
                db.commit().close()
                logging.error("".join(e.args))
                logging.error("Saved the progress of what you have downloaded.")
                sys.exit(1)
            else:
                db.commit()
            sleep(args.sleep)

            logging.info("Saving success")

        if posts:
            logging.info("スレッドの取得に成功しました")

    # HTML の変換処理
    convert()
