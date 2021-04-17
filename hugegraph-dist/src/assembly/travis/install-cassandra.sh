#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
CASS_DOWNLOAD_ADDRESS="http://archive.apache.org/dist/cassandra"
CASS_VERSION="3.10"
CASS_PACKAGE="apache-cassandra-${CASS_VERSION}"
CASS_TAR="${CASS_PACKAGE}-bin.tar.gz"
CASS_CONF="${CASS_PACKAGE}/conf/cassandra.yaml"

# download cassandra
if [ ! -f $HOME/downloads/${CASS_TAR} ]; then
  wget -q -O $HOME/downloads/${CASS_TAR} ${CASS_DOWNLOAD_ADDRESS}/${CASS_VERSION}/${CASS_TAR}
fi

# decompress cassandra
cp $HOME/downloads/${CASS_TAR} ${CASS_TAR} && tar -xzf ${CASS_TAR}

# using tmpfs for the Cassandra data directory reduces travis test runtime
sudo mkdir /mnt/ramdisk
sudo mount -t tmpfs -o size=1024m tmpfs /mnt/ramdisk
sudo ln -s /mnt/ramdisk $CASS_PACKAGE/data

# config cassandra
sed -i "s/batch_size_warn_threshold_in_kb:.*/batch_size_warn_threshold_in_kb: 10240/g" $CASS_CONF
sed -i "s/batch_size_fail_threshold_in_kb:.*/batch_size_fail_threshold_in_kb: 10240/g" $CASS_CONF

# start cassandra service
sh ${CASS_PACKAGE}/bin/cassandra
