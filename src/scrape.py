import argparse

from modules import ThreadsDownloader
from modules import ThreadsIndexer


class HelpFormatter(argparse.ArgumentDefaultsHelpFormatter,
                    argparse.RawTextHelpFormatter,
                    argparse.MetavarTypeHelpFormatter):
    def __init__(self, prog: str, indent_increment: int = 2, max_help_position: int = 24, width: int | None = None) -> None:
        super().__init__(prog, indent_increment, max_help_position, width)


parser = argparse.ArgumentParser(
    description="A script that retrieves all the thread archives from the \ URLs written in a specified JSON file.",
    epilog="",
    formatter_class=HelpFormatter,
    prog="JNVA-Scraping",
)
parser.add_argument(
    "-s",
    "--sleep",
    choices=range(1, 10),
    default=[1],
    help="specify the interval at which you download each \ HTML\n",
    metavar="[integer]",
    nargs=1,
    required=False,
    type=int,
)
parser.add_argument(
    "-j",
    "--json",
    help="choose a path for a JSON file containing thread URLs and titles\n",
    metavar="[path]",
    nargs=1,
    #required=True,
    type=str,
)
parser.add_argument(
    "-o",
    "--outdir",
    help="specify the directory in which the archive HTML \ files to be saved\n",
    metavar="[directory]",
    nargs=1,
    #required=True,
    type=str,
)
parser.add_argument(
    "--verbose",
    action="store_true",
    default=False,
    help="specify whether you want detailed information\n",
    required=False,
)
parser.add_argument(
    "--skip-index",
    action="store_true",
    default=False,
    help="specify whether you want skip retrieving latest indexes\n",
    required=False,
)
parser.add_argument(
    "--skip-download",
    action="store_true",
    default=False,
    help="specify whether you want skip retrieving latest indexes\n",
    required=False,
)
args = parser.parse_args()

if __name__ == "__main__":
    try:
        if not args.skip_index:
            ti = ThreadsIndexer(query="なんJNVA部", args=args)
            string = ti.to_string(ti.make_index())
            print(string)
            #ti.save_index()
        if not args.skip_download:
            ThreadsDownloader(args).main()
    except KeyboardInterrupt:
        pass
