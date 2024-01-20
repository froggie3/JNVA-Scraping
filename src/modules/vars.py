import logging
import os
import sys
import dotenv

dotenv.load_dotenv()
JNVADB_PATH = os.environ.get('JNVADB_PATH')

if JNVADB_PATH is not None:
    try:
        if not os.path.exists(JNVADB_PATH):
            raise FileNotFoundError
    except FileNotFoundError:
        logging.exception(
            "Sqlite3 database not found. "
            "Please run 'database_helper.py' "
            "before the execution of this program"
        )
        sys.exit(1)
