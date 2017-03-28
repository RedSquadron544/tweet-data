Twitter Data Corpus for our USC CSCI 585 Project
================================================

Scripts
-------

`stream.py` connects to the Twitter Streaming API to collect tweets in realtime and output them to stdout

`load_to_db.py` parses the output of `stream.py` and either inserts the data into a SQLite DB or a text file containing JSON objects

Usage
-----

To collect a corpus of tweets, first create a file `api_tokens.py` with 4 variables, `access_token`, `access_token_secret`, `consumer_key`, `consumer_secret`, which can be obtained from Twitter's developer site.
Then run `stream.py` and redirect its output to a file.
As long as `stream.py` is running it will be collecting tweets.
When you are finished (or just want to mess around) run `load_to_db.py`, giving it the path to the output file from `stream.py` and command line args to specify a JSON file or SQLite database for the processed output.
