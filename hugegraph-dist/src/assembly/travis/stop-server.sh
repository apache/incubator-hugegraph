#!/bin/bash

set -ev

VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
BASE_DIR=hugegraph-$VERSION
BIN=$BASE_DIR/bin

$BIN/stop-hugegraph.sh
