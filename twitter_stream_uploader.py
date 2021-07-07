import argparse
import time
from internet_scholar import AthenaLogger, read_dict_from_s3_url, AthenaDatabase, s3_file_size_in_bytes
import logging
import shutil
import boto3
import json
import sqlite3
from pathlib import Path


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
timestamp_ms bigint,
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


class TwitterStreamUploader:
    def __init__(self, s3_bucket, athena_db, aws_region):
        self.s3_bucket = s3_bucket
        self.athena_db = athena_db
        self.aws_region = aws_region

    def save_to_s3(self, delay=0):
        logging.info("BEGIN: Save twitter_stream to S3")

        if delay != 0:
            time.sleep(delay*60)
        db_name = Path(Path(__file__).parent, 'tmp', 'tweets.sqlite')
        logging.info("Obtain athena prefix from database %s", db_name)
        database = sqlite3.connect(str(db_name), isolation_level=None)
        database.row_factory = sqlite3.Row
        cursor_prefix = database.cursor()
        cursor_prefix.execute("select * from athena_prefix")
        athena_prefix = cursor_prefix.fetchone()
        database.close()

        s3 = boto3.resource('s3')

        bz2_file = Path(Path(__file__).parent, 'tmp', 'twitter_stream.json.bz2')
        orc_file = Path(Path(__file__).parent, 'tmp', 'twitter_stream.orc')
        s3_filename = "twitter_stream_raw/filter={}/creation_date={}/{}.json.bz2".format(athena_prefix['filter'],
                                                                                         athena_prefix['creation_date'],
                                                                                         'twitter_stream')
        saved = False
        if s3_file_size_in_bytes(bucket=self.s3_bucket, key=s3_filename) > bz2_file.stat().st_size:
            logging.info("Will not upload because current file on S3 is bigger")
        else:
            saved = True
            logging.info("Upload file %s to bucket %s at %s", bz2_file, self.s3_bucket, s3_filename)
            s3.Bucket(self.s3_bucket).upload_file(str(bz2_file), s3_filename)
            s3_filename = "twitter_stream/filter={}/creation_date={}/{}.orc".format(athena_prefix['filter'],
                                                                                    athena_prefix['creation_date'],
                                                                                    'twitter_stream')
            logging.info("Upload file %s to bucket %s at %s", orc_file, self.s3_bucket, s3_filename)
            s3.Bucket(self.s3_bucket).upload_file(str(orc_file), s3_filename)

        logging.info("File sizes - SQLite: %.1f Mb - BZIP2: %.1f Mb - ORC: %.1f Mb",
                     db_name.stat().st_size / 2**20,
                     bz2_file.stat().st_size / 2**20,
                     orc_file.stat().st_size / 2**20)

        logging.info("END: Save twitter_stream to S3")
        return saved

    def recreate_athena_table(self):
        logging.info("BEGIN: Recreate Athena tables for twitter_stream")

        s3_client = boto3.client('s3')
        s3_region_response = s3_client.get_bucket_location(Bucket=self.s3_bucket)
        if s3_region_response is None:
            logging.info(f"Bucket {self.s3_bucket} probably does not exist or user is not allowed to access it.")
        else:
            s3_region = s3_region_response.get('LocationConstraint','')
            if self.aws_region != s3_region:
                logging.info(f"Athena region {self.aws_region} differs from S3 region {s3_region}. "
                             f"Will not recreate tables.")
            else:
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
        twitter_stream_uploader = TwitterStreamUploader(s3_bucket=config['aws']['s3-data'],
                                                        athena_db=config['aws']['athena-data'],
                                                        aws_region=config['aws']['default_region'])
        saved_twitter_stream = twitter_stream_uploader.save_to_s3(delay=config['twitter_stream']['delay'])

        twitter_filter = TwitterFilter(twitter_filter=config['twitter_filter'],
                                       s3_bucket=config['aws']['s3-data'],
                                       athena_db=config['aws']['athena-data'])
        twitter_filter.save_to_s3()

        if saved_twitter_stream:
            twitter_stream_uploader.recreate_athena_table()
        twitter_filter.recreate_athena_table()

        total, used, free = shutil.disk_usage("/")
        logging.info("Disk Usage: total: %.1f Gb - used: %.1f Gb - free: %.1f Gb",
                     total / (2 ** 30), used / (2 ** 30), free / (2 ** 30))
    finally:
        logger.save_to_s3()
        #logger.recreate_athena_table()


if __name__ == '__main__':
    main()

