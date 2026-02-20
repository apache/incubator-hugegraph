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
TOP="$(cd "$BIN"/../ && pwd)"
GRAPH_CONF="$TOP/conf/graphs/hugegraph.properties"
WAIT_STORAGE_TIMEOUT_S=300
DETECT_STORAGE="$TOP/scripts/detect-storage.groovy"

. "$BIN"/util.sh


function key_exists {
    local key=$1
    local file_name=$2
    grep -q -E "^\s*${key}\s*=\.*" ${file_name}
}

function update_key {
    local key=$1
    local val=$2
    local file_name=$3
    sed -ri "s#^(\s*${key}\s*=).*#\\1${val}#" ${file_name}
}

function add_key {
    local key=$1
    local val=$2
    local file_name=$3
    echo "${key}=${val}" >> ${file_name}
}

# apply config from env
while IFS=' ' read -r envvar_key envvar_val; do
    if [[ "${envvar_key}" =~ hugegraph\. ]] && [[ -n ${envvar_val} ]]; then
        envvar_key=${envvar_key#"hugegraph."}
        if key_exists ${envvar_key} ${GRAPH_CONF}; then
            update_key ${envvar_key} ${envvar_val} ${GRAPH_CONF}
        else
            add_key ${envvar_key} ${envvar_val} ${GRAPH_CONF}
        fi
    fi
done < <(env | sort -r | awk -F= '{ st = index($0, "="); print $1 " " substr($0, st+1) }')

# wait for storage
if env | grep '^hugegraph\.' > /dev/null; then
    if [ -n "${WAIT_STORAGE_TIMEOUT_S:-}" ]; then
        # Extract pd.peers from config or environment
        PD_PEERS="${hugegraph_pd_peers:-}"
        if [ -z "$PD_PEERS" ]; then
            PD_PEERS=$(grep -E "^\s*pd\.peers\s*=" "$GRAPH_CONF" | sed 's/.*=\s*//' | tr -d ' ')
        fi

        if [ -n "$PD_PEERS" ]; then
            # Convert gRPC address to REST address (8686 -> 8620)
            PD_REST=$(echo "$PD_PEERS" | sed 's/:8686/:8620/g' | cut -d',' -f1)
            echo "Waiting for PD REST endpoint at $PD_REST..."

            timeout "${WAIT_STORAGE_TIMEOUT_S}s" bash -c "
                until curl -fsS http://${PD_REST}/v1/health >/dev/null 2>&1; do
                    echo 'Hugegraph server are waiting for storage backend...'
                    sleep 5
                done
                echo 'PD is reachable, waiting extra 10s for store registration...'
                sleep 10
                echo 'Storage backend is ready!'
            " || echo "Warning: Timeout waiting for storage, proceeding anyway..."
        else
            echo "No pd.peers configured, skipping storage wait..."
        fi
    fi
fi
