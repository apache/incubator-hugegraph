#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`

if [ ! -d $HOME/downloads ]; then
  mkdir $HOME/downloads
fi

if [[ "$BACKEND" == "cassandra" || "$BACKEND" == "scylladb" ]]; then
    $TRAVIS_DIR/install-cassandra.sh
elif [[ "$BACKEND" == "hbase" ]]; then
    $TRAVIS_DIR/install-hbase.sh
elif [[ "$BACKEND" == "mysql" ]]; then
    $TRAVIS_DIR/install-mysql.sh
elif [[ "$BACKEND" == "postgresql" ]]; then
    $TRAVIS_DIR/install-postgresql.sh
fi
