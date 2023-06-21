# from pprint import pprint
import os
import sqlite3
import traceback
from sqlite3 import OperationalError
from textwrap import dedent

from modules.color import Color as c


class Database:

    def connect_database(self):
        self.connect = sqlite3.connect(
            os.environ['JNVADB_PATH']
        )
        self.cursor = self.connect.cursor()
        return self

    def rollback(self):
        self.connect.rollback()

    def commit(self):
        self.connect.commit()
        return self

    def close(self):
        self.connect.close()
        return


class DBCreation(Database):

    def __thread_indexes(self):
        """
        外部ウェブサイトから取得したスレッドのインデックスを格納する
        """

        self.cursor.execute(dedent("""
        CREATE TABLE IF NOT EXISTS thread_indexes(
            server TEXT,
            bbs TEXT,
            bbskey INTEGER,
            title TEXT,
            resnum INTEGER,
            created TEXT,
            updated TEXT,
            raw_text TEXT,
            UNIQUE(bbs, bbskey)
        )
        """))
        return self

    def __messages(self):
        """
        スレッドがレコードとして格納されるテーブル
        """

        self.cursor.execute(dedent("""
        CREATE TABLE IF NOT EXISTS messages(
            bbskey INTEGER,
            number INTEGER,
            name TEXT,
            date TEXT,
            uid TEXT,
            message TEXT,
            UNIQUE(bbskey, number)
        )
        """))

        return self

    def __difference_bbskey(self):
        """
        差分のビューを作るのに必要
        """

        self.cursor.execute(dedent("""
        CREATE VIEW IF NOT EXISTS difference_bbskey AS
        SELECT
            bbskey
        FROM
            thread_indexes EXCEPT
            SELECT DISTINCT
                bbskey
            FROM
                messages
            ORDER BY
                1
        """))

        return self

    def __difference(self):
        """
        スレッドのダウンロード時に使用

        アーカイブ済みの場所にある bbskey とインデックスのそれの差分
        """

        self.cursor.execute(dedent("""
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
        self.__thread_indexes() \
            .__messages()

        return self

    def create_views(self):
        self.__difference_bbskey() \
            .__difference()

        return self


def create_database():
    """
    データベースを作成し、テーブルとビューを作成する
    """
    create_db = DBCreation()

    try:
        create_db.connect_database()

    except KeyError as e:
        print(traceback.format_exc(), end='')
        if "JNVADB_PATH" in "".join(e.args):
            print(dedent(
                f"""
                環境変数にデータベースの保存先を設定してください
                {c.GREEN + "(e.g.)" + c.RESET} echo 'export JNVADB_PATH=$HOME/.jnvadb.sqlite3' >> $HOME/.bashrc
                {c.GREEN + "(e.g.)" + c.RESET} export JNVADB_PATH=$HOME/.jnvadb.sqlite3
                """).strip())
        exit(1)

    try:
        create_db \
            .create_tables() \
            .create_views()

    except OperationalError as e:
        print(e)
        pass

    else:
        print(dedent(
            f"""
            データベースファイルの場所は {os.environ['JNVADB_PATH']} です
            正常終了しました
            """).strip())

    finally:
        create_db \
            .commit() \
            .close()


if __name__ == "__main__":

    create_database()
