#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
CASSA_DOWNLOAD_ADDRESS="http://archive.apache.org/dist/cassandra"
CASSA_VERSION="3.11.3"
CASSA_PACKAGE="apache-cassandra-${CASSA_VERSION}"
CASSA_TAR="${CASSA_PACKAGE}-bin.tar.gz"

# download cassandra
if [ ! -f $HOME/downloads/${CASSA_TAR} ]; then
  wget -q -O $HOME/downloads/${CASSA_TAR} ${CASSA_DOWNLOAD_ADDRESS}/${CASSA_VERSION}/${CASSA_TAR}
fi

# decompress cassandra
cp $HOME/downloads/${CASSA_TAR} ${CASSA_TAR} && tar xzf ${CASSA_TAR}

# config cassandra
sed -i "s/batch_size_warn_threshold_in_kb:.*/batch_size_warn_threshold_in_kb: 512/g" ${CASSA_PACKAGE}/conf/cassandra.yaml
sed -i "s/batch_size_fail_threshold_in_kb:.*/batch_size_fail_threshold_in_kb: 512/g" ${CASSA_PACKAGE}/conf/cassandra.yaml

# start cassandra service
sh ${CASSA_PACKAGE}/bin/cassandra
