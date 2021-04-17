#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
# reference:
# https://www.scylladb.com/download/?platform=ubuntu-16.04&version=scylla-4.4#open-source
# https://github.com/scylladb/gocqlx/commit/c7f0483dd30b1c7ad1972ea135528dc95cc4ce32
DOWNLOAD_ADDRESS="http://downloads.scylladb.com/deb/ubuntu/scylla-4.4-$(lsb_release -s -c).list"
SCYLLA_PORT=9042
SCYLLA_CONF="/etc/scylla/scylla.yaml"
SCYLLA_OPTS="--network-stack posix --enable-in-memory-data-store 1 --developer-mode 1"
SCYLLA_OPTS_LOG="--log-to-stdout 1 --default-log-level info"

# download and install scylladb
sudo apt-get install apt-transport-https
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 5e08fbd8b5d6ec9c
sudo curl -L --output /etc/apt/sources.list.d/scylla.list $DOWNLOAD_ADDRESS

sudo apt-get update
sudo apt-get install scylla

# config scylladb
sudo sed -i "s/batch_size_warn_threshold_in_kb:.*/batch_size_warn_threshold_in_kb: 10240/g" $SCYLLA_CONF
sudo sed -i "s/batch_size_fail_threshold_in_kb:.*/batch_size_fail_threshold_in_kb: 10240/g" $SCYLLA_CONF
cat $SCYLLA_CONF

sudo scylla_dev_mode_setup --developer-mode 1
cat /etc/scylla.d/dev-mode.conf

# setup scylladb with scylla_setup by expect
sudo apt-get install expect
sudo expect <<EOF
spawn sudo scylla_setup
expect {
    "YES/no" { send "no\r"; exp_continue }
    eof
}
EOF

# start scylladb service
#sudo /usr/bin/scylla --options-file /etc/scylla/scylla.yaml ${SCYLLA_OPTS} ${SCYLLA_OPTS_LOG} &
sudo systemctl start scylla-server || (sudo systemctl status scylla-server.service && exit 1)

# check status, wait port listened
echo "Waiting for scylladb to launch on port $SCYLLA_PORT..."
while ! nc -z localhost $SCYLLA_PORT; do
  sleep 1 # wait for 1 second before check again
done
sudo netstat -na | grep -i listen | grep tcp # can also run `nodetool status`
