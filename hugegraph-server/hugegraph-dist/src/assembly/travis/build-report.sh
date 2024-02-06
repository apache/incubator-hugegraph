#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -ev

BACKEND=$1
JACOCO_PORT=$2
JACOCO_REPORT_FILE=$3

OPTION_CLASS_FILES_BACKEND="--classfiles hugegraph-$BACKEND/target/classes/org/apache/hugegraph"
if [ "$BACKEND" == "memory" ]; then
    # hugegraph-memory is the same as hugegraph-core
    OPTION_CLASS_FILES_BACKEND=""
fi

cd hugegraph-server/hugegraph-test
mvn jacoco:dump@pull-test-data -Dapp.host=localhost -Dapp.port=$JACOCO_PORT -Dskip.dump=false
cd ../

if [[ ! -e "${TRAVIS_DIR}/jacococli.jar" ]]; then
  wget -P "${TRAVIS_DIR}" https://github.com/apache/hugegraph-doc/raw/binary-1.0/dist/server/jacococli.jar
fi

java -jar $TRAVIS_DIR/jacococli.jar report hugegraph-test/target/jacoco-it.exec \
     --classfiles hugegraph-dist/target/classes/org/apache/hugegraph \
     --classfiles hugegraph-api/target/classes/org/apache/hugegraph \
     --classfiles hugegraph-core/target/classes/org/apache/hugegraph \
     ${OPTION_CLASS_FILES_BACKEND} --xml "${JACOCO_REPORT_FILE}"
