#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
CONF=$TRAVIS_DIR/../../../../hugegraph-test/src/main/resources/hugegraph.properties

POSTGRESQL_DRIVER=org.postgresql.Driver
POSTGRESQL_URL=jdbc:postgresql://localhost:5432/
POSTGRESQL_USERNAME=postgres

# Set PostgreSQL configurations
sed -i "s/jdbc.driver=.*/jdbc.driver=$POSTGRESQL_DRIVER/" $CONF
sed -i "s?jdbc.url=.*?jdbc.url=$POSTGRESQL_URL?" $CONF
sed -i "s/jdbc.username=.*/jdbc.username=$POSTGRESQL_USERNAME/" $CONF

sudo service postgresql restart
