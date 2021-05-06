#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`

if [ ! -d $HOME/downloads ]; then
    mkdir $HOME/downloads
fi

case $BACKEND in
    cassandra)
        $TRAVIS_DIR/install-cassandra.sh
        ;;
    scylladb)
        $TRAVIS_DIR/install-scylladb.sh
        ;;
    hbase)
        $TRAVIS_DIR/install-hbase.sh
        ;;
    mysql)
        $TRAVIS_DIR/install-mysql.sh
        ;;
    postgresql)
        $TRAVIS_DIR/install-postgresql.sh
        ;;
    *)
        # don't need to install for other backends
        ;;
esac
