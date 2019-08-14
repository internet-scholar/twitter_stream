from datetime import datetime, time, timedelta
import tweepy
import json
import sqlite3
from internet_scholar import read_dict_from_s3_url, AthenaLogger, AthenaDatabase, compress, generate_orc_file
from pathlib import Path
import argparse
import logging
import uuid
import boto3
import shutil


num_exceptions = 0

STRUCTURE_TWEET_ORC = """
struct<
    created_at: timestamp,
    id: bigint,
    id_str: string,
    text: string,
    source: string,
    truncated: boolean,
    in_reply_to_status_id: bigint,
    in_reply_to_status_id_str: string,
    in_reply_to_user_id: bigint,
    in_reply_to_user_id_str: string,
    in_reply_to_screen_name: string,
    quoted_status_id: bigint,
    quoted_status_id_str: string,
    is_quote_status: boolean,
    retweet_count: int,
    favorite_count: int,
    favorited: boolean,
    retweeted: boolean,
    possibly_sensitive: boolean,
    filter_level: string,
    lang: string,
    user: struct<
        id: bigint,
        id_str: string,
        name: string,
        screen_name: string,
        location: string,
        url: string,
        description: string,
        protected: boolean,
        verified: boolean,
        followers_count: int,
        friends_count: int,
        listed_count: int,
        favourites_count: int,
        statuses_count: int,
        created_at: timestamp,
        profile_banner_url: string,
        profile_image_url_https: string,
        default_profile: boolean,
        default_profile_image: boolean,
        withheld_in_countries: array<string>,
        withheld_scope: string
    >,
    coordinates: struct<
        coordinates: array<float>,
        type: string
    >,
    place: struct<
        id: string,
        url: string,
        place_type: string,
        name: string,
        full_name: string,
        country_code: string,
        country: string,
        bounding_box: struct<
            coordinates: array<array<array<float>>>,
            type: string
        >
    >,
    entities: struct<
        hashtags: array<
            struct<
                indices: array<smallint>,
                text: string
            >
        >,
        urls: array<
            struct<
                display_url: string,
                expanded_url: string,
                indices: array<smallint>,
                url: string
            >
        >,
        user_mentions: array<
            struct<
                id: bigint,
                id_str: string,
                indices: array<smallint>,
                name: string,
                screen_name: string
            >
        >,
        symbols: array<
            struct<
                indices: array<smallint>,
                text: string
            >
        >,
        media: array<
            struct<
                display_url: string,
                expanded_url: string,
                id: bigint,
                id_str: string,
                indices: array<smallint>,
                media_url: string,
                media_url_https: string,
                source_status_id: bigint,
                source_status_id_str: string,
                type: string,
                url: string
            >
        >
    >,
    quoted_status: struct<
        created_at: timestamp,
        id: bigint,
        id_str: string,
        text: string,
        source: string,
        truncated: boolean,
        in_reply_to_status_id: bigint,
        in_reply_to_status_id_str: string,
        in_reply_to_user_id: bigint,
        in_reply_to_user_id_str: string,
        in_reply_to_screen_name: string,
        quoted_status_id: bigint,
        quoted_status_id_str: string,
        is_quote_status: boolean,
        retweet_count: int,
        favorite_count: int,
        favorited: boolean,
        retweeted: boolean,
        possibly_sensitive: boolean,
        filter_level: string,
        lang: string,
        user: struct<
            id: bigint,
            id_str: string,
            name: string,
            screen_name: string,
            location: string,
            url: string,
            description: string,
            protected: boolean,
            verified: boolean,
            followers_count: int,
            friends_count: int,
            listed_count: int,
            favourites_count: int,
            statuses_count: int,
            created_at: timestamp,
            profile_banner_url: string,
            profile_image_url_https: string,
            default_profile: boolean,
            default_profile_image: boolean,
            withheld_in_countries: array<string>,
            withheld_scope: string
        >,
        coordinates: struct<
            coordinates: array<float>,
            type: string
        >,
        place: struct<
            id: string,
            url: string,
            place_type: string,
            name: string,
            full_name: string,
            country_code: string,
            country: string,
            bounding_box: struct<
                coordinates: array<array<array<float>>>,
                type: string
            >
        >,
        entities: struct<
            hashtags: array<
                struct<
                    indices: array<smallint>,
                    text: string
                >
            >,
            urls: array<
                struct<
                    display_url: string,
                    expanded_url: string,
                    indices: array<smallint>,
                    url: string
                >
            >,
            user_mentions: array<
                struct<
                    id: bigint,
                    id_str: string,
                    indices: array<smallint>,
                    name: string,
                    screen_name: string
                >
            >,
            symbols: array<
                struct<
                    indices: array<smallint>,
                    text: string
                >
            >,
            media: array<
                struct<
                    display_url: string,
                    expanded_url: string,
                    id: bigint,
                    id_str: string,
                    indices: array<smallint>,
                    media_url: string,
                    media_url_https: string,
                    source_status_id: bigint,
                    source_status_id_str: string,
                    type: string,
                    url: string
                >
            >
        >
    >,
    retweeted_status: struct<
        created_at: timestamp,
        id: bigint,
        id_str: string,
        text: string,
        source: string,
        truncated: boolean,
        in_reply_to_status_id: bigint,
        in_reply_to_status_id_str: string,
        in_reply_to_user_id: bigint,
        in_reply_to_user_id_str: string,
        in_reply_to_screen_name: string,
        quoted_status_id: bigint,
        quoted_status_id_str: string,
        is_quote_status: boolean,
        retweet_count: int,
        favorite_count: int,
        favorited: boolean,
        retweeted: boolean,
        possibly_sensitive: boolean,
        filter_level: string,
        lang: string,
        user: struct<
            id: bigint,
            id_str: string,
            name: string,
            screen_name: string,
            location: string,
            url: string,
            description: string,
            protected: boolean,
            verified: boolean,
            followers_count: int,
            friends_count: int,
            listed_count: int,
            favourites_count: int,
            statuses_count: int,
            created_at: timestamp,
            profile_banner_url: string,
            profile_image_url_https: string,
            default_profile: boolean,
            default_profile_image: boolean,
            withheld_in_countries: array<string>,
            withheld_scope: string
        >,
        coordinates: struct<
            coordinates: array<float>,
            type: string
        >,
        place: struct<
            id: string,
            url: string,
            place_type: string,
            name: string,
            full_name: string,
            country_code: string,
            country: string,
            bounding_box: struct<
                coordinates: array<array<array<float>>>,
                type: string
            >
        >,
        entities: struct<
            hashtags: array<
                struct<
                    indices: array<smallint>,
                    text: string
                >
            >,
            urls: array<
                struct<
                    display_url: string,
                    expanded_url: string,
                    indices: array<smallint>,
                    url: string
                >
            >,
            user_mentions: array<
                struct<
                    id: bigint,
                    id_str: string,
                    indices: array<smallint>,
                    name: string,
                    screen_name: string
                >
            >,
            symbols: array<
                struct<
                    indices: array<smallint>,
                    text: string
                >
            >,
            media: array<
                struct<
                    display_url: string,
                    expanded_url: string,
                    id: bigint,
                    id_str: string,
                    indices: array<smallint>,
                    media_url: string,
                    media_url_https: string,
                    source_status_id: bigint,
                    source_status_id_str: string,
                    type: string,
                    url: string
                >
            >
        >
    >
>
"""

STRUCTURE_TWEET_ATHENA = """
created_at timestamp,
id bigint,
id_str string,
text string,
source string,
truncated boolean,
in_reply_to_status_id bigint,
in_reply_to_status_id_str string,
in_reply_to_user_id bigint,
in_reply_to_user_id_str string,
in_reply_to_screen_name string,
quoted_status_id bigint,
quoted_status_id_str string,
is_quote_status boolean,
retweet_count int,
favorite_count int,
favorited boolean,
retweeted boolean,
possibly_sensitive boolean,
filter_level string,
lang string,
user struct<
    id: bigint,
    id_str: string,
    name: string,
    screen_name: string,
    location: string,
    url: string,
    description: string,
    protected: boolean,
    verified: boolean,
    followers_count: int,
    friends_count: int,
    listed_count: int,
    favourites_count: int,
    statuses_count: int,
    created_at: timestamp,
    profile_banner_url: string,
    profile_image_url_https: string,
    default_profile: boolean,
    default_profile_image: boolean,
    withheld_in_countries: array<string>,
    withheld_scope: string
>,
coordinates struct<
    coordinates: array<float>,
    type: string
>,
place struct<
    id: string,
    url: string,
    place_type: string,
    name: string,
    full_name: string,
    country_code: string,
    country: string,
    bounding_box: struct<
        coordinates: array<array<array<float>>>,
        type: string
    >
>,
entities struct<
    hashtags: array<
        struct<
            indices: array<smallint>,
            text: string
        >
    >,
    urls: array<
        struct<
            display_url: string,
            expanded_url: string,
            indices: array<smallint>,
            url: string
        >
    >,
    user_mentions: array<
        struct<
            id: bigint,
            id_str: string,
            indices: array<smallint>,
            name: string,
            screen_name: string
        >
    >,
    symbols: array<
        struct<
            indices: array<smallint>,
            text: string
        >
    >,
    media: array<
        struct<
            display_url: string,
            expanded_url: string,
            id: bigint,
            id_str: string,
            indices: array<smallint>,
            media_url: string,
            media_url_https: string,
            source_status_id: bigint,
            source_status_id_str: string,
            type: string,
            url: string
        >
    >
>,
quoted_status struct<
    created_at: timestamp,
    id: bigint,
    id_str: string,
    text: string,
    source: string,
    truncated: boolean,
    in_reply_to_status_id: bigint,
    in_reply_to_status_id_str: string,
    in_reply_to_user_id: bigint,
    in_reply_to_user_id_str: string,
    in_reply_to_screen_name: string,
    quoted_status_id: bigint,
    quoted_status_id_str: string,
    is_quote_status: boolean,
    retweet_count: int,
    favorite_count: int,
    favorited: boolean,
    retweeted: boolean,
    possibly_sensitive: boolean,
    filter_level: string,
    lang: string,
    user: struct<
        id: bigint,
        id_str: string,
        name: string,
        screen_name: string,
        location: string,
        url: string,
        description: string,
        protected: boolean,
        verified: boolean,
        followers_count: int,
        friends_count: int,
        listed_count: int,
        favourites_count: int,
        statuses_count: int,
        created_at: timestamp,
        profile_banner_url: string,
        profile_image_url_https: string,
        default_profile: boolean,
        default_profile_image: boolean,
        withheld_in_countries: array<string>,
        withheld_scope: string
    >,
    coordinates: struct<
        coordinates: array<float>,
        type: string
    >,
    place: struct<
        id: string,
        url: string,
        place_type: string,
        name: string,
        full_name: string,
        country_code: string,
        country: string,
        bounding_box: struct<
            coordinates: array<array<array<float>>>,
            type: string
        >
    >,
    entities: struct<
        hashtags: array<
            struct<
                indices: array<smallint>,
                text: string
            >
        >,
        urls: array<
            struct<
                display_url: string,
                expanded_url: string,
                indices: array<smallint>,
                url: string
            >
        >,
        user_mentions: array<
            struct<
                id: bigint,
                id_str: string,
                indices: array<smallint>,
                name: string,
                screen_name: string
            >
        >,
        symbols: array<
            struct<
                indices: array<smallint>,
                text: string
            >
        >,
        media: array<
            struct<
                display_url: string,
                expanded_url: string,
                id: bigint,
                id_str: string,
                indices: array<smallint>,
                media_url: string,
                media_url_https: string,
                source_status_id: bigint,
                source_status_id_str: string,
                type: string,
                url: string
            >
        >
    >
>,
retweeted_status struct<
    created_at: timestamp,
    id: bigint,
    id_str: string,
    text: string,
    source: string,
    truncated: boolean,
    in_reply_to_status_id: bigint,
    in_reply_to_status_id_str: string,
    in_reply_to_user_id: bigint,
    in_reply_to_user_id_str: string,
    in_reply_to_screen_name: string,
    quoted_status_id: bigint,
    quoted_status_id_str: string,
    is_quote_status: boolean,
    retweet_count: int,
    favorite_count: int,
    favorited: boolean,
    retweeted: boolean,
    possibly_sensitive: boolean,
    filter_level: string,
    lang: string,
    user: struct<
        id: bigint,
        id_str: string,
        name: string,
        screen_name: string,
        location: string,
        url: string,
        description: string,
        protected: boolean,
        verified: boolean,
        followers_count: int,
        friends_count: int,
        listed_count: int,
        favourites_count: int,
        statuses_count: int,
        created_at: timestamp,
        profile_banner_url: string,
        profile_image_url_https: string,
        default_profile: boolean,
        default_profile_image: boolean,
        withheld_in_countries: array<string>,
        withheld_scope: string
    >,
    coordinates: struct<
        coordinates: array<float>,
        type: string
    >,
    place: struct<
        id: string,
        url: string,
        place_type: string,
        name: string,
        full_name: string,
        country_code: string,
        country: string,
        bounding_box: struct<
            coordinates: array<array<array<float>>>,
            type: string
        >
    >,
    entities: struct<
        hashtags: array<
            struct<
                indices: array<smallint>,
                text: string
            >
        >,
        urls: array<
            struct<
                display_url: string,
                expanded_url: string,
                indices: array<smallint>,
                url: string
            >
        >,
        user_mentions: array<
            struct<
                id: bigint,
                id_str: string,
                indices: array<smallint>,
                name: string,
                screen_name: string
            >
        >,
        symbols: array<
            struct<
                indices: array<smallint>,
                text: string
            >
        >,
        media: array<
            struct<
                display_url: string,
                expanded_url: string,
                id: bigint,
                id_str: string,
                indices: array<smallint>,
                media_url: string,
                media_url_https: string,
                source_status_id: bigint,
                source_status_id_str: string,
                type: string,
                url: string
            >
        >
    >
>
"""

ATHENA_CREATE_TWITTER_STREAM = """
CREATE EXTERNAL TABLE IF NOT EXISTS twitter_stream (
{structure}
)
PARTITIONED BY (filter String, creation_date String)
STORED AS ORC
LOCATION '{bucket}'
tblproperties ("orc.compress"="ZLIB");
"""

ATHENA_CREATE_TWITTER_STREAM_RAW = """
CREATE EXTERNAL TABLE IF NOT EXISTS twitter_stream_raw (
{structure}
)
PARTITIONED BY (filter String, creation_date String)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1',
  'ignore.malformed.json' = 'true'
) LOCATION '{bucket}'
TBLPROPERTIES ('has_encrypted_data'='false');
"""


class TwitterFilter:
    __CREATE_ATHENA_TABLE = """
    CREATE EXTERNAL TABLE if not exists twitter_filter (
      name string,
      track string,
      languages array<string>
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    LOCATION 's3://{s3_location}/twitter_filter/'
    """

    def __init__(self, twitter_filter, s3_bucket, athena_db):
        self.filter = twitter_filter
        self.s3_bucket = s3_bucket
        self.athena_db = athena_db

    def save_to_s3(self):
        s3_filename = "twitter_filter/{filter_name}.json".format(filter_name=self.filter['name'])
        s3 = boto3.resource('s3')
        logging.info("Save twitter filter parameters to bucket %s at %s", self.s3_bucket, s3_filename)
        s3.Object(self.s3_bucket, s3_filename).put(Body=json.dumps(self.filter))

    def recreate_athena_table(self):
        logging.info("Create Athena instance.")
        athena = AthenaDatabase(s3_output=self.s3_bucket, database=self.athena_db)
        logging.info("Delete table twitter_filter if exists")
        athena.query_athena_and_wait(query_string='DROP TABLE if exists twitter_filter')
        logging.info("Recreate table twitter_filter on %s", self.s3_bucket)
        athena.query_athena_and_wait(query_string=self.__CREATE_ATHENA_TABLE.format(s3_location=self.s3_bucket))


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
    create table if not exists tweet
        (tweet_id string,
        tweet_json string)
    """

    __SELECT_INDIVIDUAL_TWEETS = """
    select tweet_id, tweet_json
    from tweet
    where rowid in (
        select min(rowid)
        from tweet
        group by tweet_id
    )
    order by tweet_id
    """

    def __init__(self, twitter_filter, credentials, s3_bucket, athena_db):
        logging.info('Create TwitterStream object')

        # The default options for the time frame are below... in case they are changed for debugging purposes
        # start_saving_time = time(hour=0, minute=0, second=0, microsecond=0)
        # duration_saving = timedelta(days=1)
        # delay_end = timedelta(minutes=1)
        start_saving_time = time(hour=19, minute=30, second=0, microsecond=0)
        duration_saving = timedelta(minutes=1)
        delay_end = timedelta(seconds=30)

        self.start_saving = datetime.combine(datetime.utcnow().date(), start_saving_time)
        if self.start_saving <= datetime.utcnow():
            self.start_saving = self.start_saving + timedelta(days=1)
        self.end_saving = self.start_saving + duration_saving
        self.end_execution = self.end_saving + delay_end
        logging.info("Collect tweets from %s to %s. End execution at %s",
                     self.start_saving, self.end_saving, self.end_execution)

        self.creation_date = self.start_saving.strftime("%Y-%m-%d")

        self.s3_bucket = s3_bucket
        self.athena_db = athena_db

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
        logging.info("Create SQLite file if does not exist at %s", str(self.db_name))
        self.database = sqlite3.connect(self.db_name, isolation_level=None)
        logging.info("Create table for Tweets if not exist with query: %s", self.__CREATE_TABLE_TWEET)
        self.database.execute(self.__CREATE_TABLE_TWEET)
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
        self.database = sqlite3.connect(self.db_name, isolation_level=None)
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

    def save_to_s3(self):
        logging.info("BEGIN: Save twitter_stream to S3")
        logging.info("Open database again: %s", self.db_name)
        self.database = sqlite3.connect(self.db_name, isolation_level=None)
        self.database.row_factory = sqlite3.Row
        cursor_records = self.database.cursor()

        json_file = Path(Path(__file__).parent, 'tmp', 'twitter_stream.json')
        logging.info("Create JSON file %s", json_file)
        with open(json_file, 'w') as json_writer:
            logging.info("Execute query to eliminate duplicated tweets and sort by ID: %s",
                         self.__SELECT_INDIVIDUAL_TWEETS)
            cursor_records.execute(self.__SELECT_INDIVIDUAL_TWEETS)
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
                json_writer.write(json_line.rstrip("\n"))

        logging.info("Close database to release memory resources")
        self.database.close()

        s3 = boto3.resource('s3')

        logging.info("Compress JSON file before uploading to S3")
        bz2_file = compress(json_file, delete_original=False)
        s3_filename = "twitter_stream_raw/filter={}/creation_date={}/{}.json.bz2".format(self.filter['name'],
                                                                                         self.creation_date,
                                                                                         uuid.uuid4().hex)
        logging.info("Upload file %s to bucket %s at %s", bz2_file, self.s3_bucket, s3_filename)
        s3.Bucket(self.s3_bucket).upload_file(str(bz2_file), s3_filename)

        orc_file = Path(Path(__file__).parent, 'tmp', 'twitter_stream.orc')
        logging.info("Convert JSON file %s to ORC file %s", json_file, orc_file)
        generate_orc_file(filename_json=str(json_file), filename_orc=str(orc_file), structure=STRUCTURE_TWEET_ORC)
        s3_filename = "twitter_stream/filter={}/creation_date={}/{}.orc".format(self.filter['name'],
                                                                                self.creation_date,
                                                                                uuid.uuid4().hex)
        logging.info("Upload file %s to bucket %s at %s", orc_file, self.s3_bucket, s3_filename)
        s3.Bucket(self.s3_bucket).upload_file(str(orc_file), s3_filename)

        logging.info("File sizes - SQLite: %.1f Mb - JSON: %.1f Mb - BZIP2: %.1f Mb - ORC: %.1f Mb",
                     self.db_name.stat().st_size / 2**20,
                     json_file.stat().st_size / 2**20,
                     bz2_file.stat().st_size / 2**20,
                     orc_file.stat().st_size / 2**20)
        logging.info("END: Save twitter_stream to S3")

    def recreate_athena_table(self):
        logging.info("BEGIN: Recreate Athena tables for twitter_stream")
        athena = AthenaDatabase(s3_output=self.s3_bucket, database=self.athena_db)

        logging.info("Drop tables if they exist")
        athena.query_athena_and_wait(query_string="drop table if exists twitter_stream")
        athena.query_athena_and_wait(query_string="drop table if exists twitter_stream_raw")

        logging.info("Recreate tables")
        athena.query_athena_and_wait(
            query_string=ATHENA_CREATE_TWITTER_STREAM.format(
                structure=STRUCTURE_TWEET_ATHENA,
                bucket="s3://{}/twitter_stream/".format(self.s3_bucket)))
        athena.query_athena_and_wait(
            query_string=ATHENA_CREATE_TWITTER_STREAM_RAW.format(
                structure=STRUCTURE_TWEET_ATHENA,
                bucket="s3://{}/twitter_stream_raw/".format(self.s3_bucket)))

        logging.info("Add new partitions")
        athena.query_athena_and_wait(query_string="MSCK REPAIR TABLE twitter_stream")
        athena.query_athena_and_wait(query_string="MSCK REPAIR TABLE twitter_stream_raw")
        logging.info("END: Recreate Athena tables for twitter_stream")


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
                                       credentials=config['twitter_credentials'],
                                       s3_bucket=config['aws']['s3-data'],
                                       athena_db=config['aws']['athena-data'])
        twitter_stream.listen_to_tweets()
        twitter_stream.save_to_s3()

        twitter_filter = TwitterFilter(twitter_filter=config['twitter_filter'],
                                       s3_bucket=config['aws']['s3-data'],
                                       athena_db=config['aws']['athena-data'])
        twitter_filter.save_to_s3()

        twitter_stream.recreate_athena_table()
        twitter_filter.recreate_athena_table()

        total, used, free = shutil.disk_usage("/")
        logging.info("Disk Usage: total: %.1f Gb - used: %.1f Gb - free: %.1f Gb",
                     total / (2**30), used / (2**30), free / (2**30))
    finally:
        logger.save_to_s3()
        logger.recreate_athena_table()


if __name__ == '__main__':
    main()
