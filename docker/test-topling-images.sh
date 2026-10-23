#!/usr/bin/env bash
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
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
python3 - "$repo" <<'PY'
import json
import os
from pathlib import Path
import shutil
import re
import subprocess
import sys
import tempfile

repo = Path(sys.argv[1])
required = {
    "pd": {
        "HG_PD_GRPC_HOST": "pd",
        "HG_PD_RAFT_ADDRESS": "pd:8610",
        "HG_PD_RAFT_PEERS_LIST": "pd:8610",
        "HG_PD_INITIAL_STORE_LIST": "store:8500",
        "HG_PD_AUTH_SECRET_KEY": "test-secret",
    },
    "store": {
        "HG_STORE_PD_ADDRESS": "pd:8686",
        "HG_STORE_GRPC_HOST": "store",
        "HG_STORE_RAFT_ADDRESS": "store:8510",
    },
}
with tempfile.TemporaryDirectory(prefix="topling-entrypoints-") as temporary:
    for component, settings in required.items():
        directory = Path(temporary) / component
        (directory / "bin").mkdir(parents=True)
        entrypoint = repo / f"hugegraph-{component}/hg-{component}-dist/docker/docker-entrypoint.sh"
        shutil.copy(entrypoint, directory / "entrypoint.sh")
        launcher = directory / f"bin/start-hugegraph-{component}.sh"
        launcher.write_text('#!/bin/bash\nprintf \'%s\' "$SPRING_APPLICATION_JSON" > spring.json\n')
        launcher.chmod(0o755)
        environment = dict(os.environ, **settings)
        environment.pop("TOPLINGDB_ROCKSDB_PROVIDER", None)
        for provider in (None, "rocksdb", "topling"):
            if provider is not None:
                environment["TOPLINGDB_ROCKSDB_PROVIDER"] = provider
            result = subprocess.run(["bash", "entrypoint.sh"], cwd=directory, env=environment,
                                    capture_output=True, text=True)
            assert result.returncode == 0, result.stderr
            document = json.loads((directory / "spring.json").read_text())
            if provider is None:
                assert "rocksdb" not in document, "default must preserve mounted provider"
            else:
                assert document["rocksdb"]["provider"] == provider, document
        (directory / "spring.json").unlink()
        environment["TOPLINGDB_ROCKSDB_PROVIDER"] = 'topling", "unexpected":true'
        result = subprocess.run(["bash", "entrypoint.sh"], cwd=directory, env=environment,
                                capture_output=True, text=True)
        assert result.returncode != 0 and not (directory / "spring.json").exists()
        print(f"PASS: {component} provider selection, default preservation and invalid input")

    environment = dict(os.environ)
    environment.pop("TOPLING_JNI_SHA256", None)
    environment.pop("TOPLING_JNI_URL", None)
    helper = repo / "docker/prepare-topling-image.sh"
    for values in ({}, {"TOPLING_JNI_SHA256": "not-a-sha"},
                   {"TOPLING_JNI_SHA256": "a" * 64, "TOPLING_JNI_URL": "http://example.invalid/jni.jar"}):
        result = subprocess.run(["bash", str(helper)], cwd=temporary, env=dict(environment, **values),
                                capture_output=True, text=True)
        assert result.returncode != 0, "unverified artifact accepted"
        assert "Error: Topling builds require" in result.stderr, result.stderr
    print("PASS: image preparation rejects missing/untrusted artifact inputs")

    # Exercise the real image helper through preparation and profile installation,
    # with only download/native preparation stubbed (native JNI is tested separately).
    distribution = Path(temporary) / "distribution"
    (distribution / "bin").mkdir(parents=True)
    (distribution / "conf").mkdir()
    (distribution / "tools").mkdir()
    original_profile = repo / "hugegraph-server/hugegraph-dist/src/assembly/static/conf/toplingdb.yaml"
    original_bytes = original_profile.read_bytes()
    shutil.copy(original_profile, distribution / "conf/toplingdb.yaml")
    preparer = distribution / "bin/prepare-topling.sh"
    preparer.write_text("#!/bin/bash\ntest -r \"$TOPLING_JNI_JAR\" && test \"${#TOPLING_JNI_SHA256}\" -eq 64\n")
    downloader = distribution / "tools/curl"
    downloader.write_text("#!/bin/bash\nwhile (( $# )); do if [[ $1 == --output ]]; then printf stub > \"$2\"; exit 0; fi; shift; done\nexit 1\n")
    downloader.chmod(0o755)
    environment.update(TOPLING_JNI_SHA256="a" * 64, TOPLING_JNI_URL="https://example.invalid/immutable.jar")
    environment["PATH"] = str(distribution / "tools") + os.pathsep + environment["PATH"]
    result = subprocess.run(["bash", str(helper)], cwd=distribution, env=environment,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    community_profile = repo / "docker/toplingdb.yaml"
    assert (distribution / "conf/toplingdb.yaml").read_bytes() == community_profile.read_bytes()
    assert original_profile.read_bytes() == original_bytes, "distribution source profile changed"
    profile = community_profile.read_text()
    # This profile contains only indentation-based mappings. Reject duplicate
    # paths instead of silently letting the YAML reader discard an earlier key.
    seen = set()
    parents = []
    for line in profile.splitlines():
        match = re.match(r"^( *)([A-Za-z_][\w]*):", line)
        if match is None:
            continue
        indent, key = len(match[1]), match[2]
        while parents and parents[-1][0] >= indent:
            parents.pop()
        path = tuple(parent[1] for parent in parents) + (key,)
        assert path not in seen, "duplicate YAML key: " + ".".join(path)
        seen.add(path)
        parents.append((indent, key))
    classes = set(re.findall(r"^\s+class:\s*(\S+)$", profile, re.MULTILINE))
    assert classes == {"LRUCache", "SkipList", "BlockBasedTable"}, classes
    assert "CSPP" not in profile and "ToplingZip" not in profile
    for setting in ("capacity: 64M", "write_buffer_size: 16M", "max_write_buffer_number: 3",
                    "memtable_as_log_index: false", "auto_start_http: false"):
        assert setting in profile, setting
    print("PASS: community image installs small native profile and preserves full distribution profile")
PY
