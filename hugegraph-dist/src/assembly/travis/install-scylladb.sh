#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
# reference: https://github.com/scylladb/gocqlx/commit/c7f0483dd30b1c7ad1972ea135528dc95cc4ce32
DOWNLOAD_ADDRESS="http://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu"
SCYLLA_OPTS="--network-stack posix --enable-in-memory-data-store 1 --developer-mode 1"
SCYLLA_OPTS_LOG="--log-to-stdout 1 --default-log-level info"

# download and install scylladb
echo "deb [arch=amd64] $DOWNLOAD_ADDRESS trusty scylladb-1.7/multiverse" | \
    sudo tee -a /etc/apt/sources.list > /dev/null
sudo apt-get -qq update
sudo apt-get install -y --allow-unauthenticated scylla-server

# start scylladb service
sudo /usr/bin/scylla --options-file /etc/scylla/scylla.yaml ${SCYLLA_OPTS} ${SCYLLA_OPTS_LOG} &
