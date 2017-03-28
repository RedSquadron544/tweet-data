from codecs import open, decode
import sqlite3
import json
import os

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('tweets_path', nargs=1)
output_group = parser.add_mutually_exclusive_group(required=True)
output_group.add_argument('--db', dest='db_path', nargs=1)
output_group.add_argument('--json', dest='json_path', nargs=1)
opt = parser.parse_args()

with open(opt.tweets_path[0], 'r', 'utf-8') as text:
    tweets_raw = text.readlines()

    tweets_raw = [t for t in tweets_raw if t.strip()]

    tweets = []
    failed = 0
    imported = 0
    for tweet in tweets_raw:
        try:
            tweets.append(json.loads(tweet))
            imported += 1
        except:
            failed += 1
    print('Failed to import {} lines'.format(failed))
    print('Imported {} lines'.format(imported))

    failed = 0
    imported = 0

    if opt.db_path:
        try:
            os.remove(opt.db_path[0])
        except OSError:
            pass
        db = sqlite3.connect(opt.db_path[0])
        c = db.cursor()
        c.execute('CREATE TABLE tweets (id INTEGER PRIMARY KEY, text TEXT NOT NULL)')
        c.execute('CREATE TABLE hashtags (hashtag_id INTEGER PRIMARY KEY, tweet_id INTEGER NOT NULL, text TEXT NOT NULL)')
        for tweet in tweets:
            if 'id' not in tweet:
                failed += 1
                continue
            c.execute('INSERT OR IGNORE INTO tweets VALUES(?, ?)', (tweet['id'], tweet['text']))
            for hashtag in tweet['entities']['hashtags']:
                c.execute('INSERT INTO hashtags VALUES(NULL, ?, ?)', (tweet['id'], hashtag['text']))
            imported += 1
        db.commit()
        db.close()

    if opt.json_path:
        with open(opt.json_path[0], 'w', 'utf-8') as output:
            for tweet in tweets:
                if 'id' not in tweet:
                    failed += 1
                    continue
                tweet_simple = {
                    'id': tweet['id'],
                    'text': tweet['text'],
                    'hashtags': [h['text'] for h in tweet['entities']['hashtags']]
                }
                output.write(json.dumps(tweet_simple))
                output.write('\n')
                imported += 1

    print('Failed to save {} lines'.format(failed))
    print('Saved {} tweets'.format(imported))

