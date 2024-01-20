from argparse import ArgumentParser, RawTextHelpFormatter

parser = ArgumentParser(
    description="スレッドをデータベースに保存する", formatter_class=RawTextHelpFormatter
)
parser.add_argument(
    "-q",
    "--query",
    default="なんJNVA部",
    help="検索クエリを指定 (既定:「%(default)s」)",
    metavar="query",
    required=False,
)
parser.add_argument(
    "-s",
    "--skip",
    action="store_true",
    default=False,
    help="インデックスを取得しない (変換処理だけしたいとき便利です)",
    required=False,
)
parser.add_argument(
    "--convert-only",
    action="store_true",
    default=False,
    help="すでにダウンロードしたスレッドについて変換処理だけする",
    required=False,
)
parser.add_argument(
    "-t",
    "--sleep",
    default=5,
    help="どれくらいの間隔で落とすか (既定: %(default)s秒)",
    metavar="secs",
    required=False,
    type=int,
)
parser.add_argument(
    "-r",
    "--max-retry",
    default=5,
    help="HTTPエラー発生時などの最大再試行回数 (既定: %(default)s回)",
    metavar="tries",
    required=False,
    type=int,
)
parser.add_argument(
    "--force-archive", action="store_true", default=False, help="", required=False
)
args = parser.parse_args()
