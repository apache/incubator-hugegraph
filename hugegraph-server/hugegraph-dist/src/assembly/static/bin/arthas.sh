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
    echo "Usage: $0 [--pid PID] [--telnet-port PORT] [--target-ip LOOPBACK] [--command COMMAND]"
    echo "Run as the service OS user; defaults to bin/pid and loopback port 3658."
    echo "New agents disable HTTP/MCP; use the existing telnet port for an active agent."
    echo "Exiting this helper stops Arthas, including after --command or quit; the JVM keeps running."
}

BIN=$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TOP=$(cd -P "$BIN/.." && pwd)
pid=""
port=3658
target_ip=127.0.0.1
command=""
boot_args=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        --pid|--telnet-port|--target-ip|--command)
            [ "$#" -ge 2 ] || fail "Missing value for $1"
            case "$1" in
                --pid) pid=$2 ;;
                --telnet-port) port=$2 ;;
                --target-ip) target_ip=$2 ;;
                --command) command=$2; boot_args=(--command "$2") ;;
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
[ -t 0 ] || [ -n "$command" ] || fail "Interactive use needs a terminal; use --command COMMAND"
case "$target_ip" in
    [Ll][Oo][Cc][Aa][Ll][Hh][Oo][Ss][Tt]) target_ip=127.0.0.1 ;;
esac
JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java
ARTHAS="$TOP/lib/arthas"
[ -f "$ARTHAS/arthas-boot.jar" ] || fail "Bundled Arthas files are missing from lib/arthas"
boot_args+=(--arthas-home "$ARTHAS" --target-ip "$target_ip" --telnet-port "$port" --http-port -1 "$pid")
# Keep the listener within this diagnosis session. Verify its PID before stopping
# it, since an occupied port must never stop a different application's agent.
state_dir=$(mktemp -d)
client_pid=""
attaching=true
# The native client's -t option does not bound its final response wait.
bounded_client() {
    "$JAVA" -jar "$ARTHAS/arthas-client.jar" -t 5000 -c "$1" "$target_ip" "$port" &
    local command_pid=$!
    (sleep 10; kill -KILL "$command_pid" 2>/dev/null) >/dev/null 2>&1 &
    local timer_pid=$!
    local status=0
    wait "$command_pid" || status=$?
    kill -KILL "$timer_pid" 2>/dev/null || true
    wait "$timer_pid" 2>/dev/null || true
    return "$status"
}
stop_agent() {
    local session attempt
    if ! session=$(bounded_client session 2>&1); then
        if [[ "$session" == *"Connection refused"* ]]; then
            return 0
        fi
        echo "Error: could not verify Arthas cleanup at $target_ip:$port; check the agent manually" >&2
        return 1
    fi
    if [[ ! "$session" =~ JAVA_PID[[:space:]]+$pid([[:space:]]|$) ]]; then
        echo "Error: could not verify the target PID for cleanup; session must be enabled at $target_ip:$port" >&2
        return 1
    fi
    bounded_client stop >/dev/null 2>&1 || true
    # Command errors can return zero; verify closure after asynchronous shutdown.
    for attempt in 1 2 3; do
        if ! session=$(bounded_client session 2>&1) && [[ "$session" == *"Connection refused"* ]]; then
            return 0
        fi
        sleep 0.2
    done
    echo "Error: could not stop Arthas at $target_ip:$port; check session/stop settings or restart the service" >&2
    return 1
}
cleanup() {
    local status=$?
    trap '' INT TERM HUP
    trap - EXIT
    if [ -n "$client_pid" ]; then
        # Let an in-flight loadAgent finish before checking/closing its listener.
        if [ "$attaching" = false ]; then
            kill "$client_pid" 2>/dev/null || true
        fi
        wait "$client_pid" 2>/dev/null || true
    fi
    if [ -f "$state_dir/validated" ]; then
        stop_agent || { [ "$status" -ne 0 ] || status=1; }
    fi
    rm -f "$state_dir/validated"
    rmdir "$state_dir"
    exit "$status"
}
trap cleanup EXIT
trap 'trap "" INT TERM HUP; exit 130' INT
trap 'trap "" INT TERM HUP; exit 143' TERM
trap 'trap "" INT TERM HUP; exit 129' HUP
(
    # Group signals must not end the client before target-side loadAgent finishes.
    trap '' INT TERM HUP
    exec "$JAVA" --add-modules jdk.attach --class-path "$ARTHAS/arthas-core.jar" \
        "$BIN/ArthasAttach.java" "$pid" "$ARTHAS" "$port" "$TOP" "$state_dir/validated" "$target_ip"
) <&0 &
client_pid=$!
wait "$client_pid"
attaching=false
"$JAVA" -jar "$ARTHAS/arthas-boot.jar" "${boot_args[@]}" <&0 &
client_pid=$!
wait "$client_pid"
client_pid=""
