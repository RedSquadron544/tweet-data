from codecs import open, decode
import sqlite3
import json
import os
import sys

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('db_path', nargs=1)
opt = parser.parse_args()

if not opt.db_path:
    print('Error, please specify a DB path')
    sys.exit(1)

cursor = None
db = sqlite3.connect(opt.db_path[0])
cursor = db.cursor()

tweets = cursor.execute('SELECT text FROM tweets').fetchall()

for text, in tweets:
    print(text)

db.close()
