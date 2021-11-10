#!/bin/bash

set -ev

TRAVIS_DIR=$(dirname "$0")
CONF=hugegraph-test/src/main/resources/hugegraph.properties

POSTGRESQL_DRIVER=org.postgresql.Driver
POSTGRESQL_URL=jdbc:postgresql://localhost:5432/
POSTGRESQL_USERNAME=postgres

# Set PostgreSQL configurations
sed -i "s/jdbc.driver=.*/jdbc.driver=$POSTGRESQL_DRIVER/" $CONF
sed -i "s?jdbc.url=.*?jdbc.url=$POSTGRESQL_URL?" $CONF
sed -i "s/jdbc.username=.*/jdbc.username=$POSTGRESQL_USERNAME/" $CONF

sudo service postgresql stop 9.2

docker pull postgres:9.6
docker volume create pgdata
docker run --rm -v pgdata:/var/lib/postgresql/data -p 5432:5432 -e POSTGRES_PASSWORD="******" -d postgres:9.6
