from datetime import datetime, time, timedelta
import tweepy
import json
import sqlite3
from internet_scholar import read_dict_from_s3_url, AthenaLogger
from pathlib import Path
import argparse
import logging


num_exceptions = 0


class TwitterListener(tweepy.StreamListener):
    __INSERT_TWEET = """
    insert into tweet
    (tweet_id, tweet_json)
    values (?, ?)
    """

    def __init__(self, database, start_saving, end_saving, end_execution):
        super().__init__()
        self.start_saving = start_saving
        self.end_saving = end_saving
        self.end_execution = end_execution
        self.database = database

    def on_data(self, raw_data):
        data = json.loads(raw_data)
        if 'in_reply_to_status_id' in data:
            created_at = datetime.strptime(data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
            if self.start_saving <= created_at < self.end_saving:
                self.database.execute(self.__INSERT_TWEET, (data['id_str'], raw_data))
                global num_exceptions
                num_exceptions = 0
            elif created_at >= self.end_execution:
                return False


class TwitterStream:
    MAX_ATTEMPTS_TWITTER_STREAM = 10

    __CREATE_TABLE_TWEET = """
    create table tweet
        (tweet_id string,
        tweet_json string)
    """

    __CREATE_TABLE_UNIQUE_TWEET = """
    create table unique_tweet
        (tweet_id string primary key,
        tweet_json string)
    """

    __INSERT_UNIQUE_TWEET = """
    insert or ignore into unique_tweet
    (tweet_id, tweet_json)
    select tweet_id, tweet_json
    from tweet
    """

    __CREATE_TABLE_ATHENA_PREFIX = """
    create table athena_prefix
        (filter string,
        creation_date string)
    """

    __INSERT_ATHENA_PREFIX = """
    insert into athena_prefix
    (filter, creation_date)
    values (?, ?)
    """

    __SELECT_UNIQUE_TWEETS = """
    select tweet_json
    from unique_tweet
    order by tweet_id
    """

    def __init__(self, twitter_filter, credentials):
        logging.info('Create TwitterStream object')

        # The default options for the time frame are below... in case they are changed for debugging purposes
        start_saving_time = time(hour=18, minute=53, second=0, microsecond=0)
        duration_saving = timedelta(minutes=1)
        delay_end = timedelta(seconds=30)
        # start_saving_time = time(hour=0, minute=0, second=0, microsecond=0)
        # duration_saving = timedelta(days=1)
        # delay_end = timedelta(minutes=1)

        self.start_saving = datetime.combine(datetime.utcnow().date(), start_saving_time)
        if self.start_saving <= datetime.utcnow():
            self.start_saving = self.start_saving + timedelta(days=1)
        self.end_saving = self.start_saving + duration_saving
        self.end_execution = self.end_saving + delay_end
        logging.info("Collect tweets from %s to %s. End execution at %s",
                     self.start_saving, self.end_saving, self.end_execution)

        self.creation_date = self.start_saving.strftime("%Y-%m-%d")

        which_credentials = int(datetime.utcnow().timestamp() / 86400) % 2
        if which_credentials == 1:
            logging.info("Use odd days' Twitter credentials")
            credentials = credentials['odd_days']
        else:
            logging.info("Use even days' Twitter credentials")
            credentials = credentials['even_days']
        self.consumer_key = credentials['consumer_key']
        self.consumer_secret = credentials['consumer_secret']
        self.access_token = credentials['access_token']
        self.access_token_secret = credentials['access_token_secret']

        self.filter = twitter_filter
        self.filter['track_terms'] = [x.strip() for x in self.filter['track'].split(',')]
        logging.info("Track terms: %s", self.filter)

        self.db_name = Path(Path(__file__).parent, 'tmp', 'tweets.sqlite')
        Path(self.db_name).parent.mkdir(parents=True, exist_ok=True)
        logging.info("Create SQLite file if does not exist at %s", self.db_name)
        self.database = sqlite3.connect(str(self.db_name), isolation_level=None)
        logging.info("Create table for Tweets if not exist with query: %s", self.__CREATE_TABLE_TWEET)
        self.database.execute(self.__CREATE_TABLE_TWEET)
        self.database.execute(self.__CREATE_TABLE_UNIQUE_TWEET)
        self.database.execute(self.__CREATE_TABLE_ATHENA_PREFIX)
        self.database.execute(self.__INSERT_ATHENA_PREFIX, (self.filter['name'], self.creation_date))
        self.database.close()

    def __recursive_listen(self):
        try:
            logging.info("Authenticate and listen to tweets...")
            auth = tweepy.OAuthHandler(consumer_key=self.consumer_key, consumer_secret=self.consumer_secret)
            auth.set_access_token(key=self.access_token, secret=self.access_token_secret)
            api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
            my_stream_listener = TwitterListener(database=self.database,
                                                 start_saving=self.start_saving,
                                                 end_saving=self.end_saving,
                                                 end_execution=self.end_execution)
            my_stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)
            my_stream.filter(track=self.filter['track_terms'], languages=self.filter['languages'])
        except Exception as e:
            global num_exceptions
            if num_exceptions > self.MAX_ATTEMPTS_TWITTER_STREAM:
                logging.info("Exceeded maximum number of exceptions (%d) while listening to tweets: TERMINATE",
                             self.MAX_ATTEMPTS_TWITTER_STREAM)
                raise
            else:
                num_exceptions = num_exceptions + 1
                logging.info("Exception number %d while listening to tweets: resume listening", num_exceptions)
                self.__recursive_listen()

    def listen_to_tweets(self):
        self.database = sqlite3.connect(str(self.db_name), isolation_level=None)
        self.__recursive_listen()
        self.database.close()

    def __gen_dict_extract(self, key, var):
        if hasattr(var, 'items'):
            for k, v in var.items():
                if k == key:
                    yield v
                if isinstance(v, dict):
                    for result in self.__gen_dict_extract(key, v):
                        yield result
                elif isinstance(v, list):
                    for d in v:
                        for result in self.__gen_dict_extract(key, d):
                            yield result

    def prepare_database(self):
        logging.info("BEGIN: prepare database for conversion")

        logging.info("Open database again: %s", self.db_name)
        self.database = sqlite3.connect(str(self.db_name), isolation_level=None)
        self.database.row_factory = sqlite3.Row

        self.database.execute(self.__INSERT_UNIQUE_TWEET)

        json_file = Path(Path(__file__).parent, 'tmp', 'twitter_stream.json')
        logging.info("Create JSON file %s", json_file)
        with open(json_file, 'w') as json_writer:
            logging.info("Execute query to eliminate duplicated tweets and sort by ID: %s",
                         self.__SELECT_UNIQUE_TWEETS)
            cursor_records = self.database.cursor()
            cursor_records.execute(self.__SELECT_UNIQUE_TWEETS)
            logging.info("Fill JSON file with tweets")
            for record in cursor_records:
                # standardize all dates to PrestoDB/Athena format
                json_line = record['tweet_json']
                tweet_json = json.loads(json_line)
                for created_at in self.__gen_dict_extract('created_at', tweet_json):
                    json_line = json_line.replace(created_at,
                                                  datetime.strftime(datetime.strptime(created_at,
                                                                                      '%a %b %d %H:%M:%S +0000 %Y'),
                                                                    '%Y-%m-%d %H:%M:%S'),
                                                  1)
                json_writer.write("{}\n".format(json_line.strip("\r\n")))

        self.database.close()
        logging.info("END: prepare database for conversion")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='S3 Bucket with configuration', required=True)
    args = parser.parse_args()

    config = read_dict_from_s3_url(url=args.config)
    logger = AthenaLogger(app_name="twitter_stream",
                          s3_bucket=config['aws']['s3-admin'],
                          athena_db=config['aws']['athena-admin'])
    try:
        twitter_stream = TwitterStream(twitter_filter=config['twitter_filter'],
                                       credentials=config['twitter_credentials'])
        twitter_stream.listen_to_tweets()
        twitter_stream.prepare_database()
    finally:
        logger.save_to_s3()


if __name__ == '__main__':
    main()
