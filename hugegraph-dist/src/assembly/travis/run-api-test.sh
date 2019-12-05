#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`

mvn package -DskipTests
$TRAVIS_DIR/start-server.sh
mvn test -P api-test,$BACKEND
cat hugegraph-0.11.0/logs/hugegraph-server.log
$TRAVIS_DIR/build-report.sh
$TRAVIS_DIR/stop-server.sh
