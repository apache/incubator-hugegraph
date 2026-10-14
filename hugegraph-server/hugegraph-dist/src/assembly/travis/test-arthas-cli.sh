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
# Usage: bash test-arthas-cli.sh <server-arthas-packaging.jar> <store-arthas-packaging.jar>
# Real archive extraction; stub only JVM inspection/client launch. Integration
# validation should additionally attach the shipped client to a running service.
set -euo pipefail
[ "$#" -eq 2 ] || { echo "Supply Server and Store Arthas packaging JARs" >&2; exit 1; }
SERVER_PACKAGE=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
STORE_PACKAGE=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
SCRIPT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../static/bin" && pwd)/arthas.sh
JAR=$(command -v jar)
REAL_JAVA_HOME=${JAVA_HOME:-}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export HOME="$work/private home"
export JAVA_HOME="$work/test jdk"
export CLIENT_ARGS="$work/client-args"
mkdir -p "$HOME" "$JAVA_HOME/bin"
ln -s "$JAR" "$JAVA_HOME/bin/jar"
cat > "$JAVA_HOME/bin/java" <<'EOF'
#!/bin/bash
if [ "$1" = --add-modules ]; then
    printf '%s\n' "$5" >> "$CLIENT_ARGS.helpers"
    exit 0
fi
printf '%s\n' "$@" > "$CLIENT_ARGS"
EOF
cat > "$JAVA_HOME/bin/jcmd" <<'EOF'
#!/bin/bash
printf '%s\n' "$TEST_VM_COMMAND"
EOF
chmod 755 "$JAVA_HOME/bin/java" "$JAVA_HOME/bin/jcmd"

reject() {
    if "$@" > "$work/rejected.log" 2>&1; then
        echo "Unexpected success: $*" >&2
        exit 1
    fi
}
check_client() {
    local boot home
    boot=$(sed -n '2p' "$CLIENT_ARGS")
    home=$(sed -n '4p' "$CLIENT_ARGS")
    [ -s "$boot" ] && [ -s "$home/arthas-client.jar" ]
    [ "$(sed -n '5p' "$CLIENT_ARGS")" = --target-ip ]
    [ "$(sed -n '6p' "$CLIENT_ARGS")" = 127.0.0.1 ]
    [ "$(sed -n '9p' "$CLIENT_ARGS")" = --http-port ]
    [ "$(sed -n '10p' "$CLIENT_ARGS")" = -1 ]
    grep -qx 'arthas.config.overrideAll=true' "$home/arthas.properties"
    grep -qx 'arthas.ip=127.0.0.1' "$home/arthas.properties"
    grep -qx 'arthas.httpPort=-1' "$home/arthas.properties"
    grep -qx 'arthas.mcpEndpoint=' "$home/arthas.properties"
    grep -qx 'arthas.tunnelServer=' "$home/arthas.properties"
    [ "$(ls -ld "$HOME/.hugegraph-arthas" | cut -c1-10)" = drwx------ ]
    [ ! -e "$(tail -n 1 "$CLIENT_ARGS.helpers")" ]
}

server="$work/server distribution"
mkdir -p "$server/bin" "$server/lib"
cp "$SCRIPT" "$server/bin/arthas.sh"
touch "$server/bin/start-hugegraph.sh"
cp "$SERVER_PACKAGE" "$server/lib/arthas-packaging-server.jar"
echo "$$" > "$server/bin/pid"
export TEST_VM_COMMAND='java_command: org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap fixture'
bash "$server/bin/arthas.sh" --command version
check_client
[ "$(sed -n '11p' "$CLIENT_ARGS")" = --command ]
[ "$(sed -n '12p' "$CLIENT_ARGS")" = version ]
cache_count=$(find "$HOME/.hugegraph-arthas" -name arthas-boot.jar | wc -l)
bash "$server/bin/arthas.sh"
[ "$(sed -n '11p' "$CLIENT_ARGS")" = "$$" ]
[ "$cache_count" = "$(find "$HOME/.hugegraph-arthas" -name arthas-boot.jar | wc -l)" ]
reject bash "$server/bin/arthas.sh" --pid -1
reject bash "$server/bin/arthas.sh" --pid '1; echo unsafe'
reject bash "$server/bin/arthas.sh" --pid 9999999999
reject bash "$server/bin/arthas.sh" --telnet-port 65536
reject bash "$server/bin/arthas.sh" --target-ip 0.0.0.0
reject bash "$server/bin/arthas.sh" --http-port 8563
reject bash "$server/bin/arthas.sh" --tunnel-server ws://example.invalid
export TEST_VM_COMMAND='java_command: unrelated.Process'
reject bash "$server/bin/arthas.sh"
echo "Server archive, cache reuse, argument boundaries and reused PID checks passed"

store="$work/store distribution"
mkdir -p "$store/bin" "$store/lib" "$work/boot/BOOT-INF/lib"
cp "$SCRIPT" "$store/bin/arthas.sh"
touch "$store/bin/start-hugegraph-store.sh"
cp "$STORE_PACKAGE" "$work/boot/BOOT-INF/lib/arthas-packaging-store.jar"
(cd "$work/boot" && "$JAR" cf "$store/lib/hg-store-node-test.jar" BOOT-INF)
echo "$$" > "$store/bin/pid"
export TEST_VM_COMMAND="java_command: $store/lib/hg-store-node-test.jar"
bash "$store/bin/arthas.sh" --pid "$$" --telnet-port 3669 --command version
check_client
[ "$(sed -n '8p' "$CLIENT_ARGS")" = 3669 ]
export TEST_VM_COMMAND=$'jvm_args: -Dname=HugeGraphStore\njava_command: org.springframework.boot.loader.JarLauncher'
bash "$store/bin/arthas.sh" --command version
check_client
export TEST_VM_COMMAND='java_command: org.springframework.boot.loader.JarLauncher'
reject bash "$store/bin/arthas.sh"
echo "Store nested archive and both Spring Boot launcher layouts passed"
[ "$(wc -l < "$CLIENT_ARGS.helpers")" = "$(sort -u "$CLIENT_ARGS.helpers" | wc -l)" ]
echo "Private attach helper directories are unique and cleaned after each client"

export TEST_VM_COMMAND="java_command: $store/lib/hg-store-node-test.jar"
mv "$HOME/.hugegraph-arthas" "$HOME/real-cache"
ln -s "$HOME/real-cache" "$HOME/.hugegraph-arthas"
reject bash "$store/bin/arthas.sh"
rm "$HOME/.hugegraph-arthas"
mv "$HOME/real-cache" "$HOME/.hugegraph-arthas"
chmod 755 "$HOME/.hugegraph-arthas"
reject bash "$store/bin/arthas.sh"
echo "Symlink and non-private cache refusal passed"

# Exercise the actual native agent against hostile target configuration. Only
# this test-owned JVM is stopped; the distribution's service PID is never used.
python3 - "$SCRIPT" "$SERVER_PACKAGE" "$REAL_JAVA_HOME" "$work" <<'PY'
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import time

script, package, java_home, work = sys.argv[1:]
java = str(pathlib.Path(java_home) / "bin/java") if java_home else "java"
javac = str(pathlib.Path(java_home) / "bin/javac") if java_home else "javac"
root = pathlib.Path(work) / "native fixture"
for name in ("bin", "lib", "home", "classes"):
    (root / name).mkdir(parents=True, exist_ok=True)
source = root / "HugeGraphServerBootstrap.java"
source.write_text("package org.apache.hugegraph.bootstrap; "
                  "public class HugeGraphServerBootstrap { "
                  "public static void main(String[] args) throws Exception { "
                  "Thread.sleep(120000); }}")
subprocess.run([javac, "-d", str(root / "classes"), str(source)], check=True)
shutil.copy(script, root / "bin/arthas.sh")
(root / "bin/start-hugegraph.sh").touch()
shutil.copy(package, root / "lib/arthas-packaging-fixture.jar")
# Reserve distinct unused ports together, then release them immediately before
# starting the test-owned JVM. A bind collision fails the test rather than
# attaching another service.
reservations = [socket.socket() for _ in range(3)]
for reservation in reservations:
    reservation.bind(("127.0.0.1", 0))
safe_port, unsafe_telnet, unsafe_http = [s.getsockname()[1] for s in reservations]
for reservation in reservations:
    reservation.close()
unsafe = root / "unsafe.properties"
tunnel_listener = socket.socket()
tunnel_listener.bind(("127.0.0.1", 0))
tunnel_listener.listen()
tunnel_listener.settimeout(0.5)
tunnel_url = f"ws://127.0.0.1:{tunnel_listener.getsockname()[1]}/ws"
unsafe.write_text(f"arthas.config.overrideAll=true\narthas.ip=0.0.0.0\n"
                  f"arthas.telnetPort={unsafe_telnet}\narthas.httpPort={unsafe_http}\n"
                  f"arthas.mcpEndpoint=/mcp\narthas.tunnelServer={tunnel_url}\n")
target_env = dict(os.environ, ARTHAS_CONFIG_LOCATION=str(unsafe),
                  ARTHAS_CONFIG_OVERRIDE_ALL="true", ARTHAS_IP="0.0.0.0",
                  ARTHAS_TUNNELSERVER=tunnel_url, ARTHAS_TUNNEL_SERVER=tunnel_url)
target_env["arthas.config.location"] = str(unsafe)
target_env["arthas.tunnelServer"] = tunnel_url
client_env = dict(os.environ, HOME=str(root / "home"))
if java_home:
    client_env["JAVA_HOME"] = java_home
else:
    client_env.pop("JAVA_HOME", None)
client = ["bash", str(root / "bin/arthas.sh"), "--telnet-port", str(safe_port)]

def run_client(command):
    result = subprocess.run(client + ["--command", command], env=client_env,
                            text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=45)
    if result.returncode:
        raise AssertionError(result.stdout)
    return result.stdout

def can_connect(host, port):
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False

with (root / "jvm.log").open("w") as log:
    process = subprocess.Popen([java, "-Darthas.config.location=" + str(unsafe),
                                "-Darthas.config.overrideAll=true", "-Darthas.ip=0.0.0.0",
                                "-Darthas.tunnelServer=" + tunnel_url,
                                "-cp", str(root / "classes"),
                                "org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap",
                                "fixture"], env=target_env, stdout=log, stderr=subprocess.STDOUT)
    try:
        (root / "bin/pid").write_text(str(process.pid))
        time.sleep(1)
        output = run_client("version")
        assert re.search(r"\n4\.1\.5\s*\n", output), output
        assert f"Process {process.pid} already using port {safe_port}" in output, output
        assert can_connect("127.0.0.1", safe_port)
        assert not can_connect("127.0.0.1", unsafe_telnet)
        assert not can_connect("127.0.0.1", unsafe_http)
        try:
            connection, address = tunnel_listener.accept()
        except socket.timeout:
            pass
        else:
            connection.close()
            raise AssertionError(f"Arthas contacted the overridden tunnel endpoint: {address}")
        # Inspect the actual listening address, avoiding VPN/proxy redirects
        # that can make a local non-loopback connection reach loopback sockets.
        if sys.platform.startswith("linux"):
            inodes = set()
            for descriptor in pathlib.Path(f"/proc/{process.pid}/fd").iterdir():
                try:
                    link = os.readlink(descriptor)
                except FileNotFoundError:
                    continue
                if link.startswith("socket:["):
                    inodes.add(link[8:-1])
            addresses = []
            for table in ("tcp", "tcp6"):
                for line in pathlib.Path(f"/proc/{process.pid}/net/{table}").read_text().splitlines()[1:]:
                    fields = line.split()
                    address, port = fields[1].split(":")
                    if fields[3] == "0A" and fields[9] in inodes:
                        addresses.append((address, int(port, 16)))
            assert len(addresses) == 1 and addresses[0][1] == safe_port, addresses
            assert addresses[0][0] in ("0100007F", "0000000000000000FFFF00000100007F"), addresses
        else:
            listeners = subprocess.check_output(["lsof", "-nP", "-a", "-p", str(process.pid),
                                                 "-iTCP", "-sTCP:LISTEN"], text=True)
            listeners = listeners.splitlines()[1:]
            assert len(listeners) == 1 and f"127.0.0.1:{safe_port} " in listeners[0], listeners
        run_client("stop")
        assert process.poll() is None, "Stopping Arthas stopped the application JVM"
        assert not can_connect("127.0.0.1", safe_port)
        print("Native attach rejects hostile config precedence, stays local and stops without JVM restart")
    finally:
        tunnel_listener.close()
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
PY
