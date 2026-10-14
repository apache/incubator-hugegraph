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
# Offline attachment to a running Server or Store using the bundled agent.
set -euo pipefail

fail() { echo "Error: $*" >&2; exit 1; }
usage() {
    echo "Usage: $0 [--pid PID] [--telnet-port PORT] [--command COMMAND]"
    echo "Run as the service OS user; defaults to bin/pid and loopback port 3658."
    echo "New agents disable HTTP/MCP; use the existing telnet port for an active agent."
    echo "Use 'quit' to disconnect, 'stop' to stop Arthas."
}

BIN=$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TOP=$(cd -P "$BIN/.." && pwd)
pid=""
port=3658
boot_args=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        --pid|--telnet-port|--command)
            [ "$#" -ge 2 ] || fail "Missing value for $1"
            case "$1" in
                --pid) pid=$2 ;;
                --telnet-port) port=$2 ;;
                --command) boot_args=(--command "$2") ;;
            esac
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; fail "Unknown option: $1" ;;
    esac
done
if [ -z "$pid" ]; then
    [ -r "$BIN/pid" ] || fail "No bin/pid; start the service or supply --pid PID"
    pid=$(cat "$BIN/pid")
fi
[[ "$pid" =~ ^[1-9][0-9]{0,9}$ ]] || fail "PID must be a positive integer"
[[ "$port" =~ ^[1-9][0-9]{0,4}$ ]] && [ "$port" -le 65535 ] || fail "Invalid telnet port"
JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java
ARTHAS="$TOP/lib/arthas"
[ -f "$ARTHAS/arthas-boot.jar" ] || fail "Bundled Arthas files are missing from lib/arthas"
"$JAVA" --add-modules jdk.attach --class-path "$ARTHAS/arthas-core.jar" \
    "$BIN/ArthasAttach.java" "$pid" "$ARTHAS" "$port" "$TOP"
boot_args+=(--arthas-home "$ARTHAS" --target-ip 127.0.0.1 --telnet-port "$port" --http-port -1 "$pid")
"$JAVA" -jar "$ARTHAS/arthas-boot.jar" "${boot_args[@]}"
