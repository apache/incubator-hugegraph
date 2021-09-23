#!/bin/bash

ETCD_VER=v3.5.0
ETCD_BIN=/tmp/test-etcd

GOOGLE_URL=https://storage.googleapis.com/etcd
GITHUB_URL=https://github.com/etcd-io/etcd/releases/download

DOWNLOAD_URL=${GITHUB_URL}

rm -f /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
rm -rf $ETCD_BIN
mkdir -p $ETCD_BIN

curl -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz

tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz -C $ETCD_BIN --strip-components=1

$ETCD_BIN/etcd --version

#$ETCD_BIN/etcd
nohup $ETCD_BIN/etcd &
