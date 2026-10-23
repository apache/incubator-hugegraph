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
PY
