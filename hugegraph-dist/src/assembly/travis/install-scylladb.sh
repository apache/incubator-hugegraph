#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
# reference:
# https://www.scylladb.com/download/?platform=ubuntu-16.04&version=scylla-4.4#open-source
# https://github.com/scylladb/gocqlx/commit/c7f0483dd30b1c7ad1972ea135528dc95cc4ce32
DOWNLOAD_ADDRESS="http://downloads.scylladb.com/deb/ubuntu/scylla-4.4-$(lsb_release -s -c).list"
SCYLLA_OPTS="--network-stack posix --enable-in-memory-data-store 1 --developer-mode 1"
SCYLLA_OPTS_LOG="--log-to-stdout 1 --default-log-level info"

# download and install scylladb
sudo apt-get install apt-transport-https
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 5e08fbd8b5d6ec9c
sudo curl -L --output /etc/apt/sources.list.d/scylla.list $DOWNLOAD_ADDRESS

sudo apt-get update
sudo apt-get install scylla

cat /etc/scylla/scylla.yaml

# start scylladb service
#sudo /usr/bin/scylla --options-file /etc/scylla/scylla.yaml ${SCYLLA_OPTS} ${SCYLLA_OPTS_LOG} &
sudo systemctl start scylla-server
