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
# Called from the stopped component directory during an image build only.
set -euo pipefail

if [[ -z "${TOPLING_JNI_URL:-}" || -z "${TOPLING_JNI_SHA256:-}" ]] &&
   [[ -f /tmp/topling-runtime.env ]]; then
    requested_url="${TOPLING_JNI_URL:-}"
    requested_sha="${TOPLING_JNI_SHA256:-}"
    # Checked-in, reviewed artifact defaults; explicit build inputs win.
    source /tmp/topling-runtime.env
    TOPLING_JNI_URL="${requested_url:-${TOPLING_JNI_URL:-}}"
    TOPLING_JNI_SHA256="${requested_sha:-${TOPLING_JNI_SHA256:-}}"
fi

[[ "${TOPLING_JNI_SHA256:-}" =~ ^[[:xdigit:]]{64}$ ]] || {
    echo 'Error: Topling builds require a trusted TOPLING_JNI_SHA256' >&2
    exit 1
}
export TOPLING_JNI_SHA256

if [[ -r /run/secrets/topling_jni ]]; then
    export TOPLING_JNI_JAR=/run/secrets/topling_jni
else
    [[ "${TOPLING_JNI_URL:-}" == https://* ]] || {
        echo 'Error: Topling builds require an HTTPS TOPLING_JNI_URL or a topling_jni build secret' >&2
        exit 1
    }
    download_dir=$(mktemp -d)
    trap 'rm -rf "$download_dir"' EXIT
    curl --fail --location --retry 3 --proto '=https' --proto-redir '=https' \
        --output "$download_dir/rocksdbjni.jar" "$TOPLING_JNI_URL"
    export TOPLING_JNI_JAR="$download_dir/rocksdbjni.jar"
fi
# The shared preparer verifies the hash, ELF/dependencies, and real EasyMigrate
# behavior before installing a component-local runtime and checksum receipt.
bash bin/prepare-topling.sh

# Community images need no proprietary factory registration. Keep the full
# distribution profile unchanged and install the image's smaller native profile.
cp "$(dirname "${BASH_SOURCE[0]}")/toplingdb.yaml" conf/toplingdb.yaml
