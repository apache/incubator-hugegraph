#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`

if [[ "$BACKEND" == "cassandra" || "$BACKEND" == "scylladb" ]]; then
    $TRAVIS_DIR/install-cassandra.sh
elif [ "$BACKEND" == "hbase" ]; then
    $TRAVIS_DIR/install-hbase.sh
elif [ "$BACKEND" == "mysql" ]; then
    $TRAVIS_DIR/install-mysql.sh
fi
