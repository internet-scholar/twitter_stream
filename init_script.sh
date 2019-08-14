#!/bin/bash
sudo timedatectl set-timezone UTC
sudo apt update -y
sudo apt install -y python3-pip openjdk-8-jre
cd /home/ubuntu
mkdir tmp
wget http://repo1.maven.org/maven2/org/apache/orc/orc-tools/1.5.6/orc-tools-1.5.6-uber.jar -P ./tmp/
wget https://raw.githubusercontent.com/internet-scholar/twitter_stream/master/requirements.txt
wget https://raw.githubusercontent.com/internet-scholar/twitter_stream/master/twitter_stream.py
wget https://raw.githubusercontent.com/internet-scholar/internet_scholar/master/internet_scholar.py
wget https://raw.githubusercontent.com/internet-scholar/twitter_stream/master/requirements.txt -O requirements2.txt
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements.txt
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements2.txt
mkdir /home/ubuntu/.aws
printf "[default]\\nregion={region}" > /home/ubuntu/.aws/config
#python3 /home/ubuntu/twitter_stream.py -c $1 && sudo shutdown -h now