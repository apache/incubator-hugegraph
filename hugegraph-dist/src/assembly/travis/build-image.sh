#!/usr/bin/env bash
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

BASE_DIR=$(cd "$(dirname "$0")" && pwd -P)
PROJECT_PATH="$(cd "${BASE_DIR}/../../../.." && pwd -P)"
PROJECT_POM_PATH="${PROJECT_PATH}/pom.xml"

mvn -f "${PROJECT_POM_PATH}" clean package -DskipTests

CONTEXT_PATH=$(mvn -f "${PROJECT_POM_PATH}" -q -N \
    org.codehaus.mojo:exec-maven-plugin:1.3.1:exec \
    -Dexec.executable='echo' -Dexec.args='${final.name}')
    CONTEXT_PATH="${PROJECT_PATH}/${CONTEXT_PATH}"

if [ -n "$2" ]; then
    wget $2 -O /tmp/hugegraph-hubble.tar.gz
    tar -zxf /tmp/hugegraph-hubble.tar.gz -C $CONTEXT_PATH
    docker build -t $1 $CONTEXT_PATH -f $PROJECT_PATH/DockerfileWithHubble
else
    docker build -t $1 $CONTEXT_PATH -f $PROJECT_PATH/Dockerfile
fi
