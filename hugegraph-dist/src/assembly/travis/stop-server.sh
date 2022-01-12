#!/bin/bash

set -ev

BASE_DIR=$1
BIN=$BASE_DIR/bin
VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`

$BIN/stop-hugegraph.sh
