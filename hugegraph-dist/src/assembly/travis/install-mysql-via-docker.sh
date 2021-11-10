#!/bin/bash

set -ev

TRAVIS_DIR=$(dirname "$0")

# Need speed up it
CONF=hugegraph-test/src/main/resources/hugegraph.properties
MYSQL_USERNAME=root

# Set MySQL configurations
sed -i "s/jdbc.username=.*/jdbc.username=$MYSQL_USERNAME/" $CONF
sed -i "s/jdbc.reconnect_max_times=.*/jdbc.reconnect_max_times=10/" $CONF
sed -i "s/jdbc.reconnect_interval=.*/jdbc.reconnect_interval=5/" $CONF


# Keep for upgrade in future
#docker pull mysql:5.7
#docker run -p 3306:3306 --name mysql -e MYSQL_ROOT_PASSWORD="******" -d mysql:5.7
