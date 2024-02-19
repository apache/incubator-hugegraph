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

# create a folder to save the docker-related file
DOCKER_FOLDER='./docker'
mkdir -p $DOCKER_FOLDER

INIT_FLAG_FILE="init_complete"

if [ ! -f "${DOCKER_FOLDER}/${INIT_FLAG_FILE}" ]; then
    # wait for storage backend
    ./bin/wait-storage.sh
    if [ -z "$PASSWORD" ]; then
        echo "init hugegraph with non-auth mode"
        ./bin/init-store.sh
    else
        echo "init hugegraph with auth mode"
        ./bin/enable-auth.sh
        echo "$PASSWORD" | ./bin/init-store.sh
    fi
    # create a flag file to avoid re-init when restarting
    touch ${DOCKER_FOLDER}/${INIT_FLAG_FILE}
else
    echo "Hugegraph Initialization already done. Skipping re-init..."
fi

# start hugegraph
# remove "-g zgc" now, which is only available on ARM-Mac with java > 13 
./bin/start-hugegraph.sh -j "$JAVA_OPTS" 

tail -f /dev/null
