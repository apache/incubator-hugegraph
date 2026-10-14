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
# Offline, same-user attachment to a running Server or Store. No service restart.
set -euo pipefail
umask 077

fail() { echo "Error: $*" >&2; exit 1; }
usage() {
    echo "Usage: $0 [--pid PID] [--telnet-port PORT] [--command COMMAND]"
    echo "Run as the service OS user; defaults to bin/pid and loopback port 3658."
    echo "New agents disable HTTP/MCP; use the existing telnet port for an active agent."
    echo "Use 'quit' to disconnect, 'stop' to stop Arthas."
}

BIN=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TOP=$(cd "$BIN/.." && pwd)
pid=""
port=3658
diagnostic_command=""
has_command=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --pid|--telnet-port|--command)
            [ "$#" -ge 2 ] || fail "Missing value for $1"
            case "$1" in
                --pid) pid=$2 ;;
                --telnet-port) port=$2 ;;
                --command) diagnostic_command=$2; has_command=true ;;
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
kill -0 "$pid" 2>/dev/null || fail "PID $pid is not running or accessible"
owner=$(ps -o uid= -p "$pid" | tr -d '[:space:]')
[ "$owner" = "$(id -u)" ] || fail "Run as the service OS user"

JAVA=java
JAR=jar
JCMD=jcmd
if [ -n "${JAVA_HOME:-}" ]; then
    JAVA="$JAVA_HOME/bin/java"
    JAR="$JAVA_HOME/bin/jar"
    JCMD="$JAVA_HOME/bin/jcmd"
fi
for tool in "$JAVA" "$JAR" "$JCMD"; do
    command -v "$tool" >/dev/null 2>&1 || fail "JDK tool unavailable: $tool (set JAVA_HOME)"
done
vm_command=$("$JCMD" "$pid" VM.command_line) || fail "Cannot inspect target JVM; check OS attach permissions"
if [ -f "$BIN/start-hugegraph-store.sh" ]; then
    case "$vm_command" in
        *"java_command: $TOP/lib/hg-store-node-"*.jar*) ;;
        *"java_command: org.springframework.boot.loader.JarLauncher"*)
            [[ "$vm_command" == *"-Dname=HugeGraphStore"* ]] || fail "PID is not a Store JVM" ;;
        *) fail "PID is not this Store JVM" ;;
    esac
elif [ -f "$BIN/start-hugegraph.sh" ]; then
    [[ "$vm_command" == *"java_command: org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap "* ]] ||
        fail "PID is not a Server JVM"
else
    fail "Run this script from a Server or Store distribution bin directory"
fi

# Keep loaded agent JARs available after the client exits. The cache is private
# to this OS user, outside world-writable temporary directories and dist upgrades.
[ -n "${HOME:-}" ] && [ -d "$HOME" ] || fail "HOME must identify the service user's home"
cache="$HOME/.hugegraph-arthas"
[ ! -L "$cache" ] || fail "Refusing symlink cache: $cache"
if [ ! -e "$cache" ]; then
    mkdir "$cache" || fail "Cannot create private cache: $cache"
fi
[ -d "$cache" ] && [ -O "$cache" ] || fail "Cache must belong to the current OS user"
permissions=$(LC_ALL=C ls -ld "$cache")
case "$permissions" in
    drwx------\ *|drwx------.\ *) ;;
    *) fail "Cache must have mode 700 without additional ACLs: $cache" ;;
esac

sources=("$TOP"/lib/arthas-packaging-*.jar)
nested=""
if [ "${#sources[@]}" -eq 1 ] && [ -f "${sources[0]}" ]; then
    source_jar=${sources[0]}
else
    sources=("$TOP"/lib/hg-store-node-*.jar)
    [ "${#sources[@]}" -eq 1 ] && [ -f "${sources[0]}" ] || fail "Expected one bundled Arthas package"
    source_jar=${sources[0]}
    nested=$("$JAR" tf "$source_jar" | awk '/^BOOT-INF\/lib\/arthas-packaging-[^\/]+[.]jar$/')
    [[ "$nested" =~ ^BOOT-INF/lib/arthas-packaging-[^/]+\.jar$ ]] || fail "Expected one nested Arthas package"
fi
read -r checksum size < <(cksum < "$source_jar")
arthas_home="$cache/attach-v3-$checksum-$size-$port"
if [ ! -e "$arthas_home" ]; then
    staging=$(mktemp -d "$cache/.prepare.XXXXXX")
    trap 'rm -rf "$staging"' EXIT
    package=$source_jar
    if [ -n "$nested" ]; then
        (cd "$staging" && "$JAR" xf "$source_jar" "$nested")
        package="$staging/$nested"
    fi
    (cd "$staging" && "$JAR" xf "$package" arthas-bin.zip)
    mkdir "$staging/home"
    (cd "$staging/home" && "$JAR" xf "$staging/arthas-bin.zip")
    # Override target-JVM defaults as well as the upstream packaging defaults.
    cat > "$staging/home/arthas.properties" <<EOF
arthas.config.overrideAll=true
arthas.ip=127.0.0.1
arthas.telnetPort=$port
arthas.httpPort=-1
arthas.mcpEndpoint=
arthas.tunnelServer=
arthas.sessionTimeout=1800
EOF
    # Concurrent clients may already have installed this identical package.
    if ! mkdir "$arthas_home" 2>/dev/null; then
        fail "Cache preparation in progress; retry the command"
    fi
    mv "$staging/home" "$arthas_home/bin"
fi
[ ! -L "$arthas_home" ] && [ -O "$arthas_home" ] || fail "Unsafe Arthas cache"
arthas_home="$arthas_home/bin"
for artifact in arthas-boot.jar arthas-core.jar arthas-agent.jar arthas-spy.jar arthas-client.jar; do
    [ -s "$arthas_home/$artifact" ] || fail "Incomplete bundled Arthas cache: $arthas_home"
done
echo "Connecting to PID $pid on 127.0.0.1:$port (new agents disable HTTP/MCP)."
# Pin the native config location and put this private configuration first.
# FeatureCodec drops blank values, so the file must also win for empty MCP and
# tunnel settings. Boot connects only after this native attachment completes.
helper_dir=$(mktemp -d "$cache/.attach.XXXXXX")
helper="$helper_dir/LocalArthasAttach.java"
trap 'rm -rf "$helper_dir"; if [ -n "${staging:-}" ]; then rm -rf "$staging"; fi' EXIT
cat > "$helper" <<'JAVA'
import com.sun.tools.attach.VirtualMachine;
import com.taobao.arthas.core.config.FeatureCodec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

class LocalArthasAttach {
    public static void main(String[] args) throws Exception {
        Path home = Path.of(args[1]);
        Map<String, String> config = new LinkedHashMap<>();
        config.put("config.overrideAll", "true");
        config.put("config.location", home.resolve("arthas.properties").toString());
        config.put("home", home.toString());
        config.put("ip", "127.0.0.1");
        config.put("telnetPort", args[2]);
        config.put("httpPort", "-1");
        config.put("mcpEndpoint", "");
        config.put("tunnelServer", "");
        String agentArgs = home.resolve("arthas-core.jar") + ";" +
                           FeatureCodec.DEFAULT_COMMANDLINE_CODEC.toString(config);
        VirtualMachine vm = VirtualMachine.attach(args[0]);
        try {
            vm.loadAgent(home.resolve("arthas-agent.jar").toString(),
                         URLEncoder.encode(agentArgs, StandardCharsets.UTF_8));
        } finally {
            vm.detach();
        }
    }
}
JAVA
"$JAVA" --add-modules jdk.attach --class-path "$arthas_home/arthas-core.jar" "$helper" "$pid" "$arthas_home" "$port"
boot_args=(--arthas-home "$arthas_home" --target-ip 127.0.0.1 --telnet-port "$port" --http-port -1)
if [ "$has_command" = true ]; then
    boot_args+=(--command "$diagnostic_command")
fi
"$JAVA" -jar "$arthas_home/arthas-boot.jar" "${boot_args[@]}" "$pid"
