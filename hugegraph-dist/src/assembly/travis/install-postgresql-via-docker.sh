#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
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
