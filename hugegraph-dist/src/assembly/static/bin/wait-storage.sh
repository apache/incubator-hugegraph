#!/bin/bash
#
# Copyright 2023 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
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
WAIT_STORAGE_TIMEOUT_S=120
DETECT_STORAGE="$TOP/scripts/detect-storage.groovy"

. "$BIN"/util.sh

# apply config from env
while IFS=' ' read -r envvar_key envvar_val; do
    if [[ "${envvar_key}" =~ hugegraph\. ]] && [[ ! -z ${envvar_val} ]]; then
        envvar_key=${envvar_key#"hugegraph."}
        if grep -q -E "^\s*${envvar_key}\s*=\.*" ${GRAPH_CONF}; then
            sed -ri "s#^(\s*${envvar_key}\s*=).*#\\1${envvar_val}#" ${GRAPH_CONF}
        else
            echo "${envvar_key}=${envvar_val}" >> ${GRAPH_CONF}
        fi
    else
        continue
    fi
done < <(env | sort -r | awk -F= '{ st = index($0, "="); print $1 " " substr($0, st+1) }')

# wait for storage
if env | grep '^hugegraph\.' > /dev/null; then
    if ! [ -z "${WAIT_STORAGE_TIMEOUT_S:-}" ]; then
        timeout "${WAIT_STORAGE_TIMEOUT_S}s" bash -c \
        "until bin/gremlin-console.sh -- -e $DETECT_STORAGE > /dev/null 2>&1; do echo \"waiting for storage...\"; sleep 5; done"
    fi
fi
