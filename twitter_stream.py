from internet_scholar import InternetScholar
import uuid
import argparse
import json
import os
from twarc import Twarc
import sqlite3
import time
import tweepy
from datetime import timezone
from datetime import datetime, timedelta


# TODO: add logging

init_script_converter = """#!/usr/bin/env bash
sudo timedatectl set-timezone UTC
cd ~
mkdir utils
wget http://repo1.maven.org/maven2/org/apache/orc/orc-tools/1.5.6/orc-tools-1.5.6-uber.jar -P ./utils/
sudo apt update -y
sudo apt install -y python3-pip openjdk-8-jre
wget https://raw.githubusercontent.com/internet-scholar/twitter_stream_converter/master/requirements.txt
wget https://raw.githubusercontent.com/internet-scholar/twitter_stream_converter/master/twitter_stream_converter.py
wget https://raw.githubusercontent.com/internet-scholar/internet_scholar/master/internet_scholar.py
pip3 install --trusted-host pypi.python.org -r ~/requirements.txt
mkdir .aws
printf "[default]\\nregion={region}" > ~/.aws/config
python3 twitter_stream_converter.py -c {config} -k {key} --logfile {prod}
sudo shutdown -h now"""

init_script_stream = """#!/usr/bin/env bash
sudo timedatectl set-timezone UTC
cd ~
sudo apt update -y
sudo apt install -y python3-pip
wget https://raw.githubusercontent.com/internet-scholar/twitter_stream/master/requirements.txt
wget https://raw.githubusercontent.com/internet-scholar/twitter_stream/master/twitter_stream.py
wget https://raw.githubusercontent.com/internet-scholar/internet_scholar/master/internet_scholar.py
pip3 install --trusted-host pypi.python.org -r ~/requirements.txt
mkdir .aws
printf "[default]\\nregion={region}" > ~/.aws/config
crontab -l | {{ cat; echo "5 0 * * * python3 twitter_stream.py upload --config {config} --convert_remotely --logfile {prod}"; }} | crontab -
python3 twitter_stream.py track --config {config} --name {filter_name} --terms {terms} {languages} --logfile --index {index} {prod} {tweepy}"""

create_table_twitter_filter = """
CREATE EXTERNAL TABLE if not exists twitter_filter (
  name string,
  track string,
  languages array<string>,
  method string,
  created_at timestamp
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{bucket}/twitter_filter/'
"""

create_table_tweet = """
create table if not exists tweet
    (filter_name string,
    creation_date timestamp,
    tweet_id string,
    tweet_json string)
"""

insert_tweet = """
insert into tweet
(filter_name, creation_date, tweet_id, tweet_json)
values (?, ? ,?, ?)
"""

num_exceptions_tweepy = 0


class MyStreamListener(tweepy.StreamListener):
    # the constructor receives the filter_name because it will be part of the record that will store each tweet
    # and will serve to partition the table twitter_streaming
    def __init__(self, filter_name, even, odd):
        super().__init__()
        self.filter_name = filter_name
        self.even = even
        self.odd = odd

    # method that is executed for each tweet that arrives through the stream
    def on_status(self, status):
        # if the number of days since Jan 1, 1970 is an even number, inserts new tweet on database "even"
        if int(status.created_at.replace(tzinfo=timezone.utc).timestamp() / 86400) % 2 == 0:
            self.even.execute(insert_tweet, (self.filter_name,
                                             datetime.strftime(status.created_at, '%Y-%m-%d'),
                                             status.id_str,
                                             json.dumps(status._json)))
        # otherwise, inserts new tweet on database "odd"
        else:
            self.odd.execute(insert_tweet, (self.filter_name,
                                            datetime.strftime(status.created_at, '%Y-%m-%d'),
                                            status.id_str,
                                            json.dumps(status._json)))

        # If it is able to receive at least one tweet, reinitialize global variable num_exceptions.
        global num_exceptions_tweepy
        num_exceptions_tweepy = 0


class TwitterStream (InternetScholar):
    MAX_ATTEMPTS_TWITTER_STREAM = 10

    def __init__(self, config_bucket, location="twitter_stream",
                 logger_name="twitter_stream", prod=True, log_file=True):
        super(TwitterStream, self).__init__(config_bucket, location=location, logger_name=logger_name,
                                            prod=prod, log_file=log_file)
        self.filter_name = None
        self.track = None
        self.languages = None
        # create both even and odd databases.
        self.database_dir = os.path.join(self.local_path, 'db')
        self.logger.info("Create directory for database: %s", self.database_dir)
        os.makedirs(self.database_dir, exist_ok=True)
        self.even_db = os.path.join(self.database_dir, 'even.sqlite')
        self.logger.info("Create sqlite db for even days: %s", self.even_db)
        self.even = sqlite3.connect(self.even_db, isolation_level=None)
        self.even.execute(create_table_tweet)
        self.odd_db = os.path.join(self.database_dir, 'odd.sqlite')
        self.logger.info("Create sqlite db for odd days: %s", self.odd_db)
        self.odd = sqlite3.connect(self.odd_db, isolation_level=None)
        self.odd.execute(create_table_tweet)

    def save_filter_arguments(self, filter_name, track, languages=None, method="twarc"):
        self.filter_name = filter_name
        self.track = track
        self.languages = languages
        self.method = method
        # create a string with all the information that we want to upload
        filter_json = {
            'name': filter_name,
            'track': track,
            'languages': languages,
            'method': method,
            'created_at': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
        }
        json_line = json.dumps(filter_json)
        self.logger.info('New filter: %s', json_line)

        # save the information to local file
        filename_json = os.path.join(self.temp_dir, '{}.json'.format(filter_name))
        self.logger.info('Temporary file with info about new filter: %s', filename_json)
        with open(filename_json, 'w') as json_file:
            json_file.write("{}\n".format(json_line))

        s3_filename = "twitter_filter/{}.json.bz2".format(filter_name)
        filename_bz2 = self.compress(filename=filename_json, delete_original=True)
        self.s3.Bucket(self.config['s3']['raw']).upload_file(filename_bz2, s3_filename)
        os.remove(filename_bz2)

        # creates table twitter_filter on Amazon Athena
        self.query_athena_and_wait(query_string='drop table if exists twitter_filter')
        self.query_athena_and_wait(query_string=create_table_twitter_filter.format(bucket=self.config['s3']['raw']))

    def listen_to_tweets_tweepy(self, index=0):
        global num_exceptions_tweepy
        try:
            # Authenticate with Twitter Stream API
            self.logger.info('Read parameters.')
            auth = tweepy.OAuthHandler(consumer_key=self.credentials['twitter'][index]['consumer_key'],
                                       consumer_secret=self.credentials['twitter'][index]['consumer_secret'])
            auth.set_access_token(key=self.credentials['twitter'][index]['access_token'],
                                  secret=self.credentials['twitter'][index]['access_token_secret'])
            api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
            self.logger.info('Twitter authenticated.')
            track_terms = [x.strip() for x in self.track.split(',')]

            # Initialize stream
            my_stream_listener = MyStreamListener(filter_name=self.filter_name, even=self.even, odd=self.odd)
            my_stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)
            self.logger.info('Listening tweets...')
            my_stream.filter(track=track_terms, languages=self.languages)
        # For any exception try to reinitialize the stream MAX_ATTEMPTS times before exiting
        except Exception as e:
            if num_exceptions_tweepy > self.MAX_ATTEMPTS_TWITTER_STREAM:
                self.logger.info('It is going to terminate: %s.', repr(e))
                raise
            else:
                num_exceptions_tweepy = num_exceptions_tweepy + 1
                self.logger.info('Exception number %d: %s', num_exceptions_tweepy, repr(e))
                self.listen_to_tweets_tweepy()

    def listen_to_tweets_twarc(self, index=0, num_exceptions=0):
        try:
            self.logger.info('Authenticate on Twitter.')
            twitter = Twarc(self.credentials['twitter'][index]['consumer_key'],
                            self.credentials['twitter'][index]['consumer_secret'],
                            self.credentials['twitter'][index]['access_token'],
                            self.credentials['twitter'][index]['access_token_secret'])

            self.logger.info('Listen to tweets')
            for tweet in twitter.filter(track=self.track, lang=self.languages):
                # if the number of days since Jan 1, 1970 is an even number, inserts new tweet on database "even"
                created_at = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                if int(created_at.timestamp() / 86400) % 2 == 0:
                    self.even.execute(insert_tweet, (self.filter_name,
                                                     datetime.strftime(created_at, '%Y-%m-%d'),
                                                     tweet['id_str'],
                                                     json.dumps(tweet)))
                # otherwise, inserts new tweet on database "odd"
                else:
                    self.odd.execute(insert_tweet, (self.filter_name,
                                                    datetime.strftime(created_at, '%Y-%m-%d'),
                                                    tweet['id_str'],
                                                    json.dumps(tweet)))
                num_exceptions = 0
        # For any exception try to reinitialize the stream MAX_ATTEMPTS times before exiting
        except Exception as e:
            if num_exceptions > self.MAX_ATTEMPTS_TWITTER_STREAM:
                self.logger.info('It is going to terminate: %s', repr(e))
                raise
            else:
                self.logger.info('Exception number %d: %s', num_exceptions, repr(e))
                self.listen_to_tweets_twarc(num_exceptions + 1)

    def listen_to_tweets(self, filter_name, track, languages=None, tweepy=False, index=0):
        track_terms = ' '.join(track)
        if tweepy:
            method = "tweepy"
        else:
            method = "twarc"
        self.save_filter_arguments(filter_name=filter_name, track=track_terms, languages=languages, method=method)
        if tweepy:
            self.listen_to_tweets_tweepy(index)
        else:
            self.listen_to_tweets_twarc(index)

    def upload_database_to_s3(self, convert_remotely=False):
        # if the number of days since Jan 1, 1970 is an even number, sends the data on database "odd" to S3
        # (since database "odd" is probably idle now)
        s3_filename = "twitter_stream_sqlite/{}/{}".format(
            datetime.strftime(datetime.utcnow() - timedelta(1), '%Y-%m-%d'), str(uuid.uuid4()))
        if int(datetime.utcnow().timestamp() / 86400) % 2 == 0:
            self.logger.info('Upload odd.sqlite as {} to {}'.format(s3_filename, self.config['s3']['raw']))
            self.s3.Bucket(self.config['s3']['raw']).upload_file(self.odd_db, s3_filename)
            self.logger.info("Delete table 'tweet' in odd.sqlite")
            self.odd.execute('drop table tweet')
            self.logger.info("Recreate table 'tweet' in odd.sqlite")
            self.odd.execute(create_table_tweet)
        # otherwise, send the data on database "even" to S3
        else:
            self.logger.info('Upload even.sqlite as {} to {}'.format(s3_filename, self.config['s3']['raw']))
            self.s3.Bucket(self.config['s3']['raw']).upload_file(self.even_db, s3_filename)
            self.logger.info("Delete table 'tweet' in even.sqlite")
            self.even.execute('drop table tweet')
            self.logger.info("Recreate table 'tweet' in even.sqlite")
            self.even.execute(create_table_tweet)
        if convert_remotely:
            self.logger.info("Instantiate EC2 that will generate ORC file")
            init_script = init_script_converter.format(region=self.credentials['aws']['default_region'],
                                                       config=self.config_bucket,
                                                       key=s3_filename,
                                                       prod="--prod" if self.prod else "")
            self.logger.info("Script: %s", init_script)
            self.instantiate_ec2(instance_type="t3a.micro", init_script=init_script, name="twitter_stream_converter")
        self.logger.info("END - Upload to S3")

    def remote_track(self, filter_name, track, languages=[], tweepy=False, index=0):
        init_script = init_script_stream.format(region=self.credentials['aws']['default_region'],
                                                config=self.config_bucket,
                                                prod="--prod" if self.prod else "",
                                                filter_name=filter_name,
                                                index=index,
                                                terms=' '.join(track),
                                                languages=("--languages " if languages else "") + " ".join(languages),
                                                tweepy="--tweepy" if tweepy else "")
        self.instantiate_ec2(init_script=init_script, name="{} ({})".format(filter_name,"twitter_stream"))


def main():
    # Configures argparse arguments
    parser = argparse.ArgumentParser()
    sp = parser.add_subparsers(dest='command')
    sp_track = sp.add_parser('track', help='Track tweets')
    sp_track.add_argument('-c', '--config', help='<Required> S3 Bucket with config', required=True)
    sp_track.add_argument('-i', '--index', type=int, help="Credentials' index", default=0)
    sp_track.add_argument('-n', '--name', help='<Required> Filter name', required=True)
    sp_track.add_argument('-t', '--terms', nargs='+', help='<Required> Track terms', required=True)
    sp_track.add_argument('-l', '--languages', nargs='+', help='Languages', default=[])
    sp_track.add_argument('--tweepy', help='Collect tweets using Tweepy (otherwise it defaults to Twarc)',
                          action='store_true')
    sp_track.add_argument('--prod', help='Save data on production database (not dev)', action='store_true')
    sp_track.add_argument('--logfile', help='Output log to file (not console)', action='store_true')
    sp_upload = sp.add_parser('upload', help='Upload tweets to S3 bucket')
    sp_upload.add_argument('-c', '--config', help='<Required> S3 Bucket with config', required=True)
    sp_upload.add_argument('--convert_remotely', help='Convert to ORC in a EC2 instance', action='store_true')
    sp_upload.add_argument('--prod', help='Save data on production database (not dev)', action='store_true')
    sp_upload.add_argument('--logfile', help='Output log to file (not console)', action='store_true')
    sp_remote = sp.add_parser('remote', help='Create an EC2 instance to track Twitter stream')
    sp_remote.add_argument('-c', '--config', help='<Required> S3 Bucket with config', required=True)
    sp_remote.add_argument('-i', '--index', type=int, help="Credentials' index", default=0)
    sp_remote.add_argument('-n', '--name', help='<Required> Filter name', required=True)
    sp_remote.add_argument('-t', '--terms', nargs='+', help='<Required> Track terms', required=True)
    sp_remote.add_argument('-l', '--languages', nargs='+', help='Languages', default=[])
    sp_remote.add_argument('--tweepy', help='Collect tweets using Tweepy (otherwise it defaults to Twarc)',
                           action='store_true')
    sp_remote.add_argument('--prod', help='Save data on production database (not dev)', action='store_true')
    sp_remote.add_argument('--logfile', help='Output log to file (not console)', action='store_true')
    args = parser.parse_args()

    twitter_stream = None
    if args.command == "track":
        try:
            twitter_stream = TwitterStream(config_bucket=args.config, logger_name="track_tweets",
                                           prod=args.prod, log_file=args.logfile)
            twitter_stream.listen_to_tweets(filter_name=args.name, track=args.terms,
                                            languages=args.languages, tweepy=args.tweepy)
        except Exception:
            twitter_stream.logger.exception("Error!")
        finally:
            twitter_stream.save_logs()
    elif args.command == "upload":
        try:
            twitter_stream = TwitterStream(config_bucket=args.config, logger_name="upload_tweets",
                                           prod=args.prod, log_file=args.logfile)
            twitter_stream.upload_database_to_s3(convert_remotely=args.convert_remotely)
        except Exception:
            twitter_stream.logger.exception("Error!")
        finally:
            twitter_stream.save_logs(recreate_log_table=False)
    elif args.command == "remote":
        try:
            twitter_stream = TwitterStream(config_bucket=args.config, logger_name="remote_track",
                                           prod=args.prod, log_file=args.logfile)
            twitter_stream.remote_track(filter_name=args.name, track=args.terms,
                                        languages=args.languages, tweepy=args.tweepy)
        except Exception:
            twitter_stream.logger.exception("Error!")
        finally:
            twitter_stream.save_logs()


if __name__ == '__main__':
    main()
