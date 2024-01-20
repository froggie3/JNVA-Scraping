#!/usr/bin/env python3
# from pprint import pprint
import os
import traceback
from sqlite3 import OperationalError
from textwrap import dedent

from modules.color import Color as c
from modules.classes import Database


class DBCreation(Database):

    def __thread_indexes(self):
        """
        外部ウェブサイトから取得したスレッドのインデックスを格納する
        """

        self.cursor.execute(dedent(
            """
            CREATE TABLE IF NOT EXISTS thread_indexes(
                server TEXT NOT NULL,
                bbs TEXT NOT NULL,
                bbskey INTEGER NOT NULL,
                title TEXT NOT NULL,
                resnum INTEGER NOT NULL,
                created TEXT NOT NULL,
                updated TEXT NOT NULL,
                raw_text TEXT NOT NULL,
                UNIQUE(bbs, bbskey)
            )
            """))

        self.cursor.execute(
            'ALTER TABLE thread_indexes ADD is_live INTEGER NOT NULL DEFAULT 1')

        return self

    def __messages(self):
        """
        スレッドがレコードとして格納されるテーブル
        """

        self.cursor.execute(dedent(
            """
            CREATE TABLE IF NOT EXISTS messages(
                bbskey INTEGER NOT NULL,
                number INTEGER NOT NULL,
                name TEXT NOT NULL,
                date TEXT NOT NULL,
                uid TEXT NOT NULL,
                message TEXT NOT NULL,
                UNIQUE(bbskey, number)
            )
            """))

        return self

    def __difference_bbskey(self):
        """
        差分をとるのに必要なビューを作成する

        テーブル `messages` にある `bbskey` カラムにあるレコードの重複を除くと、変換済みのスレッドの `bbskey` を抽出できる。
        それをテーブル thread_indexes 側の `bbskey` と差分を取ったものが変換するべきスレッドといえる。
        しかし、それでは更新分に対応できないので、
        - `is_live = 1` かつ
        - 埋められていないスレッド（レスが1002未満） かつ
        - APIから拾える `is_live = 1` の値が毎回変動するので、14 日以内でないスレッドは既に過去ログにあるものとして無視し、
        これらに合致する `bbskey` を抽出した上で、`UNION` 句でそれと合成し取得するビューを作成する。
        """
        # self.cursor.execute(
        #     'DROP VIEW IF EXISTS difference_bbskey')

        self.cursor.execute(dedent(
            '''
            CREATE VIEW difference_bbskey AS
            SELECT bbskey FROM thread_indexes
                EXCEPT SELECT DISTINCT bbskey FROM messages
            UNION
            SELECT bbskey FROM thread_indexes
                WHERE is_live = 1 AND resnum < 1002 AND datetime('now','-14 days') < updated
            ORDER BY 1
            '''))

        return self

    def __difference(self):
        """
        スレッドのダウンロード時に使用

        アーカイブ済みの場所にある bbskey とインデックスのそれの差分
        """

        self.cursor.execute(dedent(
            """
            CREATE VIEW IF NOT EXISTS difference AS
            SELECT
                server,
                bbs,
                thread_indexes.bbskey,
                title
            FROM
                difference_bbskey
                INNER JOIN
                    thread_indexes
                ON  thread_indexes.bbskey = difference_bbskey.bbskey
            """))

        return self

    def create_tables(self):
        self.__thread_indexes()
        self.__messages()

        return self

    def create_views(self):
        self.__difference_bbskey()
        self.__difference()

        return self


def create_database():
    """
    データベースを作成し、テーブルとビューを作成する
    """

    try:
        create_db = DBCreation()

    except KeyError as error:
        print(traceback.format_exc(), end='')
        if "JNVADB_PATH" in "".join(error.args):
            print(dedent(
                f"""
                環境変数にデータベースの保存先を設定してください
                {c.GREEN + "(e.g.)" + c.RESET} echo 'export JNVADB_PATH=$HOME/.jnvadb.sqlite3' >> $HOME/.bashrc
                {c.GREEN + "(e.g.)" + c.RESET} export JNVADB_PATH=$HOME/.jnvadb.sqlite3
                """).strip())
        exit(1)

    try:
        create_db.create_tables().create_views()

    except OperationalError as error:
        print(error)

    else:
        print(dedent(
            f"""
            データベースファイルの場所は {os.environ.get('JNVADB_PATH')} です
            正常終了しました
            """).strip())

    finally:
        create_db.commit().close()


if __name__ == "__main__":

    create_database()
