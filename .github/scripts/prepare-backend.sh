#!/bin/bash
# This script prepares the backend CI environment based on the specified backend type.
# It handles special build requirements for specific backends before invoking the
# standard installation script.
#
# Usage: prepare-backend.sh <backend_type>
#   where <backend_type> is one of: memory, rocksdb, hbase, hstore, etc.
set -e

BACKEND=$1

echo "Preparing backend ci environment for $BACKEND..."

if [[ "$BACKEND" == "hstore" ]]; then
  mvn package -U -Dmaven.javadoc.skip=true -Dmaven.test.skip=true -ntp
fi

$TRAVIS_DIR/install-backend.sh $BACKEND
jps -l
