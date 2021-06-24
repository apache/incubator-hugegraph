#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`

# Need speed up it
CONF=hugegraph-test/src/main/resources/hugegraph.properties
MYSQL_USERNAME=root
MYSQL_PASSWORD=123456
# Set MySQL configurations
sed -i "s/jdbc.username=.*/jdbc.username=$MYSQL_USERNAME/" $CONF
sed -i "s/jdbc.password=.*/jdbc.password=$MYSQL_PASSWORD/" $CONF

docker pull mysql:5.7
docker run -p 3306:3306 --name mysql -e MYSQL_ROOT_PASSWORD=123456 -d mysql:5.7
