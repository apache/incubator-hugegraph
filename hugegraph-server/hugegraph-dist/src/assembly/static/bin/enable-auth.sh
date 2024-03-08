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

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    cd -P "$(dirname "$SOURCE")" && pwd
}

BIN=$(abs_path)
TOP="$(cd "${BIN}"/../ && pwd)"
CONF="$TOP/conf"

GREMLIN_SERVER_CONF="gremlin-server.yaml"
REST_SERVER_CONF="rest-server.properties"
GRAPH_CONF="hugegraph.properties"

# make a backup
BAK_CONF="$TOP/conf-bak"
if [ ! -d "$BAK_CONF" ]; then
    mkdir -p "$BAK_CONF"
    cp "${CONF}/${GREMLIN_SERVER_CONF}" "${BAK_CONF}/${GREMLIN_SERVER_CONF}.bak"
    cp "${CONF}/${REST_SERVER_CONF}" "${BAK_CONF}/${REST_SERVER_CONF}.bak"
    cp "${CONF}/graphs/${GRAPH_CONF}" "${BAK_CONF}/${GRAPH_CONF}.bak"


    sed -i -e '$a\authentication: {' \
        -e '$a\  authenticator: org.apache.hugegraph.auth.StandardAuthenticator,' \
        -e '$a\  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler,' \
        -e '$a\  config: {tokens: conf/rest-server.properties}' \
        -e '$a\}' ${CONF}/${GREMLIN_SERVER_CONF}

    sed -i -e '$a\auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator' \
        -e '$a\auth.graph_store=hugegraph' ${CONF}/${REST_SERVER_CONF}

    sed -i 's/gremlin.graph=org.apache.hugegraph.HugeFactory/gremlin.graph=org.apache.hugegraph.auth.HugeFactoryAuthProxy/g' ${CONF}/graphs/${GRAPH_CONF}
fi
