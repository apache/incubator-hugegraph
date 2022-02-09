#!/bin/bash

set -ev

if [[ $# -ne 1 ]]; then
    echo "Must pass BACKEND type of hugegraph"
    exit 1
fi

BACKEND=$1
TRAVIS_DIR=`dirname $0`

if [ ! -d $HOME/downloads ]; then
    mkdir $HOME/downloads
fi

case $BACKEND in
    hstore)
        $TRAVIS_DIR/install-hstore.sh
        ;;
    *)
        # don't need to install for other backends
        ;;
esac
