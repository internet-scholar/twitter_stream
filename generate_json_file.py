from pathlib import Path
import sqlite3
import json
from datetime import datetime


__SELECT_UNIQUE_TWEETS = """
select tweet_json
from unique_tweet
order by tweet_id
"""


def __gen_dict_extract(key, var):
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                yield v
            if isinstance(v, dict):
                for result in __gen_dict_extract(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in __gen_dict_extract(key, d):
                        yield result


db_name = Path(Path(__file__).parent, 'tmp', 'tweets.sqlite')
database = sqlite3.connect(str(db_name), isolation_level=None)
database.row_factory = sqlite3.Row

json_file = Path(Path(__file__).parent, 'tmp', 'twitter_stream.json')
with open(json_file, 'w') as json_writer:
    cursor_records = database.cursor()
    cursor_records.execute(__SELECT_UNIQUE_TWEETS)
    for record in cursor_records:
        # standardize all dates to PrestoDB/Athena format
        json_line = record['tweet_json']
        tweet_json = json.loads(json_line)
        for created_at in __gen_dict_extract('created_at', tweet_json):
            json_line = json_line.replace(created_at,
                                          datetime.strftime(datetime.strptime(created_at,
                                                                              '%a %b %d %H:%M:%S +0000 %Y'),
                                                            '%Y-%m-%d %H:%M:%S'),
                                          1)
        json_writer.write("{}\n".format(json_line.strip("\r\n")))

database.close()
