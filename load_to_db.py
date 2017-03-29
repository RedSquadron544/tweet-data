from codecs import open, decode
import sqlite3
import json
import os
import sys

import argparse
parser = argparse.ArgumentParser()
output_group = parser.add_mutually_exclusive_group(required=True)
output_group.add_argument('--db', dest='db_path')
output_group.add_argument('--json', dest='json', action='store_const', const=True)
opt = parser.parse_args()


cursor = None
if opt.db_path:
    try:
        os.remove(opt.db_path[0])
    except OSError:
        pass
    db = sqlite3.connect(opt.db_path[0])
    cursor = db.cursor()
    cursor.execute('CREATE TABLE tweets (id INTEGER PRIMARY KEY, text TEXT NOT NULL)')
    cursor.execute('CREATE TABLE hashtags (hashtag_id INTEGER PRIMARY KEY, tweet_id INTEGER NOT NULL, text TEXT NOT NULL)')


def write_tweet(tweet):
    if cursor is not None:
        cursor.execute('INSERT OR IGNORE INTO tweets VALUES(?, ?)', (tweet['id'], tweet['text']))
        for hashtag in tweet['entities']['hashtags']:
            cursor.execute('INSERT INTO hashtags VALUES(NULL, ?, ?)', (tweet['id'], hashtag['text']))

    if opt.json:
        tweet_simple = {
            'id': tweet['id'],
            'text': tweet['text'],
            'hashtags': [h['text'] for h in tweet['entities']['hashtags']]
        }
        print(json.dumps(tweet_simple))

failed = 0
imported = 0

for tweet_raw in sys.stdin:
    tweet_raw = tweet_raw.strip()
    if not tweet_raw:
        continue

    try:
        tweet = json.loads(tweet_raw)

        if 'id' not in tweet:
            failed += 1
            continue

        write_tweet(tweet)
        imported += 1
    except:
        failed += 1

if opt.db_path:
    db.commit()
    db.close()

print('Failed to save {} lines'.format(failed))
print('Saved {} tweets'.format(imported))

