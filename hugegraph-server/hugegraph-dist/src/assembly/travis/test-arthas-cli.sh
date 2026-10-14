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
# Usage: bash test-arthas-cli.sh <built-server-distribution>
# Real native attachment; all processes and files below belong to this test.
set -euo pipefail
[ "$#" -eq 1 ] || { echo "Supply a built Server distribution" >&2; exit 1; }
python3 - "$1" <<'PY'
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

built = pathlib.Path(sys.argv[1]).resolve()
java_home = os.environ.get("JAVA_HOME")
java = str(pathlib.Path(java_home) / "bin/java") if java_home else "java"
javac = str(pathlib.Path(java_home) / "bin/javac") if java_home else "javac"
with tempfile.TemporaryDirectory(prefix="arthas-test-") as work:
    root = pathlib.Path(work) / "native fixture"
    for name in ("bin", "lib", "classes", "client-temp"):
        (root / name).mkdir(parents=True)
    for name in ("arthas.sh", "ArthasAttach.java"):
        shutil.copy(built / "bin" / name, root / "bin" / name)
    (root / "lib/arthas").symlink_to(built / "lib/arthas", target_is_directory=True)
    (root / "bin/start-hugegraph.sh").touch()
    source = root / "HugeGraphServerBootstrap.java"
    source.write_text("package org.apache.hugegraph.bootstrap; "
                      "public class HugeGraphServerBootstrap { "
                      "public static void main(String[] args) throws Exception { "
                      "Thread.sleep(120000); }}")
    subprocess.run([javac, "-d", str(root / "classes"), str(source)], check=True)
    reservations = [socket.socket() for _ in range(3)]
    for reservation in reservations:
        reservation.bind(("127.0.0.1", 0))
    safe_port, unsafe_telnet, unsafe_http = [s.getsockname()[1] for s in reservations]
    for reservation in reservations:
        reservation.close()
    tunnel = socket.socket()
    tunnel.bind(("127.0.0.1", 0))
    tunnel.listen()
    tunnel.settimeout(0.5)
    tunnel_url = f"ws://127.0.0.1:{tunnel.getsockname()[1]}/ws"
    unsafe = root / "unsafe.properties"
    unsafe.write_text(f"arthas.config.overrideAll=true\narthas.ip=0.0.0.0\n"
                      f"arthas.telnetPort={unsafe_telnet}\narthas.httpPort={unsafe_http}\n"
                      f"arthas.mcpEndpoint=/mcp\narthas.tunnelServer={tunnel_url}\n")
    target_env = dict(os.environ, ARTHAS_CONFIG_LOCATION=str(unsafe),
                      ARTHAS_CONFIG_OVERRIDE_ALL="true", ARTHAS_CONFIG_OVERRIDEALL="true", ARTHAS_IP="0.0.0.0",
                      ARTHAS_TUNNELSERVER=tunnel_url, ARTHAS_TUNNEL_SERVER=tunnel_url)
    target_env["arthas.config.location"] = str(unsafe)
    target_env["arthas.tunnelServer"] = tunnel_url
    client_env = dict(os.environ, JAVA_TOOL_OPTIONS=f'-Djava.io.tmpdir="{root / "client-temp"}"')
    script = ["bash", str(root / "bin/arthas.sh")]
    client = script + ["--telnet-port", str(safe_port)]

    def run(arguments, success=True):
        result = subprocess.run(arguments, env=client_env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=45)
        assert (result.returncode == 0) == success, result.stdout
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
            for options in (["--pid", "-1"], ["--pid", "1; unsafe"], ["--pid", "9999999999"],
                            ["--telnet-port", "65536"], ["--target-ip", "0.0.0.0"],
                            ["--http-port", "8563"], ["--tunnel-server", tunnel_url]):
                run(script + options, success=False)
            # A live PID from a different component must not receive the agent.
            (root / "bin/start-hugegraph.sh").unlink()
            (root / "bin/start-hugegraph-store.sh").touch()
            run(client + ["--command", "version"], success=False)
            (root / "bin/start-hugegraph-store.sh").unlink()
            (root / "bin/start-hugegraph.sh").touch()
            output = run(client + ["--pid", str(process.pid), "--command", "version"])
            assert re.search(r"\n4\.1\.5\s*\n", output), output
            assert f"Process {process.pid} already using port {safe_port}" in output, output
            assert can_connect("127.0.0.1", safe_port)
            assert not can_connect("127.0.0.1", unsafe_telnet)
            assert not can_connect("127.0.0.1", unsafe_http)
            try:
                connection, address = tunnel.accept()
            except socket.timeout:
                pass
            else:
                connection.close()
                raise AssertionError(f"Arthas contacted the overridden tunnel endpoint: {address}")
            # Inspect actual bind addresses; VPNs may redirect connection probes.
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
                                                     "-iTCP", "-sTCP:LISTEN"], text=True).splitlines()[1:]
                assert len(listeners) == 1 and f"127.0.0.1:{safe_port} " in listeners[0], listeners
            run(client + ["--command", "stop"])
            assert process.poll() is None, "Stopping Arthas stopped the application JVM"
            assert not can_connect("127.0.0.1", safe_port)
            assert not list((root / "client-temp").glob("hugegraph-arthas-*.properties"))
            print("PASS: PID/argument boundaries, hostile config, local-only agent, no tunnel, stop without restart")
        finally:
            tunnel.close()
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
PY
