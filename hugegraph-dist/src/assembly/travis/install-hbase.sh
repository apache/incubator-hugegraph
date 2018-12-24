#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
HBASE_DOWNLOAD_ADDRESS="http://archive.apache.org/dist/hbase"
HBASE_VERSION="2.0.2"
HBASE_PACKAGE="hbase-${HBASE_VERSION}"
HBASE_TAR="${HBASE_PACKAGE}-bin.tar.gz"

# download hbase
if [ ! -f $HOME/downloads/${HBASE_TAR} ]; then
  sudo wget -q -O $HOME/downloads/${HBASE_TAR} ${HBASE_DOWNLOAD_ADDRESS}/${HBASE_VERSION}/${HBASE_TAR}
fi

# decompress hbase
sudo cp $HOME/downloads/${HBASE_TAR} ${HBASE_TAR} && tar xzf ${HBASE_TAR}

# using tmpfs for the Hbase data directory reduces travis test runtime
sudo mkdir /mnt/ramdisk
sudo mount -t tmpfs -o size=1024m tmpfs /mnt/ramdisk
sudo ln -s /mnt/ramdisk /tmp/hbase

# config hbase
sudo cp -f $TRAVIS_DIR/hbase-site.xml ${HBASE_PACKAGE}/conf

# start hbase service
sudo ${HBASE_PACKAGE}/bin/start-hbase.sh
