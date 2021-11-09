#!/bin/bash

set -ev

if [[ $# -ne 1 ]]; then
    echo "Must pass BACKEND type of hugegraph"
    exit 1
fi

BACKEND=$1
TRAVIS_DIR=$(dirname "$0")

if [ ! -d "$HOME"/downloads ]; then
    mkdir "$HOME"/downloads
fi

case $BACKEND in
    cassandra)
        "$TRAVIS_DIR"/install-cassandra.sh
        ;;
    scylladb)
        "$TRAVIS_DIR"/install-scylladb.sh
        ;;
    hbase)
        "$TRAVIS_DIR"/install-hbase.sh
        ;;
    mysql)
        "$TRAVIS_DIR"/install-mysql-via-docker.sh
        ;;
    postgresql)
        "$TRAVIS_DIR"/install-postgresql-via-docker.sh
        ;;
    *)
        # don't need to install for other backends
        ;;
esac
