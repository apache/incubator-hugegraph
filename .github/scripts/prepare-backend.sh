#!/bin/bash
set -e

BACKEND=$1

echo "Preparing backend ci environment for $BACKEND..."

if [[ "$BACKEND" == "hstore" ]]; then
  mvn package -U -Dmaven.javadoc.skip=true -Dmaven.test.skip=true -ntp
fi

$TRAVIS_DIR/install-backend.sh $BACKEND
jps -l
