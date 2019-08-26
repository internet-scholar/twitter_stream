#!/bin/bash
rm ./tmp/twitter_stream.json
pip3 install awscli --upgrade --user
echo "Type the S3 filename for the database backup, followed by [ENTER]:"
read s3_filename
aws s3 cp ./tmp/tweets.sqlite $s3_filename