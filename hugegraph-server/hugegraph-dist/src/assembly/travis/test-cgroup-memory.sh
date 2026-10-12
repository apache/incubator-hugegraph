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

# Verify hard-limit sizing without starting HugeGraph or requiring root.
set -euo pipefail
UTIL=${1:?Usage: $0 UTIL_SH}
# The fixture supplies util.sh; calc_xmx invokes the free_memory hooks below.
# shellcheck disable=SC1090,SC2329
source "$UTIL"
FIXTURE=$(mktemp -d)
trap 'rm -rf "$FIXTURE"' EXIT
ROOT="$FIXTURE/cgroup"
MEMBERSHIP="$FIXTURE/membership"
MOUNTINFO="$FIXTURE/mountinfo"
mkdir -p "$ROOT"
assert_memory() {
    local expected=$1 actual
    actual=$(cgroup_free_memory "$ROOT" "$MEMBERSHIP" "$MOUNTINFO")
    [[ "$actual" == "$expected" ]] || {
        echo "expected cgroup MiB '$expected', got '$actual'" >&2
        exit 1
    }
}
# No controller keeps the original host fallback.
assert_memory ""
printf '0::/\n' > "$MEMBERSHIP"
printf 'max\n' > "$ROOT/memory.max"
printf '12345\n' > "$ROOT/memory.current"
assert_memory ""
# Four GiB limit, one GiB already used: three GiB remain, not host RAM.
printf '4294967296\n' > "$ROOT/memory.max"
printf '1073741824\n' > "$ROOT/memory.current"
assert_memory 3072
# Usage at/over limit must not become a negative or wrapped number.
printf '4294967296\n' > "$ROOT/memory.current"
assert_memory 0
printf '5368709120\n' > "$ROOT/memory.current"
assert_memory 0
# A finite ancestor constrains an unlimited child.
mkdir -p "$ROOT/team/service"
printf '0::/team/service\n' > "$MEMBERSHIP"
printf 'max\n' > "$ROOT/team/service/memory.max"
printf '0\n' > "$ROOT/team/service/memory.current"
printf '2147483648\n' > "$ROOT/team/memory.max"
printf '536870912\n' > "$ROOT/team/memory.current"
printf '1073741824\n' > "$ROOT/memory.current"
assert_memory 1536
# A stricter child wins; usage is per-level, not subtracted twice.
printf '1073741824\n' > "$ROOT/team/service/memory.max"
printf '268435456\n' > "$ROOT/team/service/memory.current"
assert_memory 768
# Host-relative membership in a private namespace falls back to mount root.
printf '0::/not-visible/container\n' > "$MEMBERSHIP"
assert_memory 3072
# Malformed limits do not introduce arithmetic errors or an artificial zero.
printf 'invalid\n' > "$ROOT/memory.max"
assert_memory ""
# v1 has different names and a numeric unlimited sentinel.
mkdir -p "$ROOT/memory/job"
printf '5:cpu,cpuacct:/job\n6:memory:/job\n' > "$MEMBERSHIP"
printf '9223372036854771712\n' > "$ROOT/memory/memory.limit_in_bytes"
printf '0\n' > "$ROOT/memory/memory.usage_in_bytes"
printf '2147483648\n' > "$ROOT/memory/job/memory.limit_in_bytes"
printf '536870912\n' > "$ROOT/memory/job/memory.usage_in_bytes"
assert_memory 1536
# Keep launcher min/max semantics; insufficient effective memory fails.
printf '0::/unified\n6:memory:/job\n' > "$MEMBERSHIP"
assert_memory 1536
# A combined v1 controller can live outside /sys/fs/cgroup/memory.
CUSTOM="$FIXTURE/combined"
mkdir -p "$CUSTOM/job"
cp "$ROOT/memory/job/"memory.* "$CUSTOM/job/"
printf '29 23 0:26 / %s rw - cgroup cgroup rw,cpu,memory\n' "$CUSTOM" > "$MOUNTINFO"
assert_memory 1536
# The mounted controller root may already be a delegated ancestor.
printf '6:memory:/delegated/job\n' > "$MEMBERSHIP"
printf '29 23 0:26 /delegated %s rw - cgroup cgroup rw,memory\n' "$CUSTOM" > "$MOUNTINFO"
assert_memory 1536
# Mountinfo escapes spaces, including custom controller mount points.
SPACED="$FIXTURE/combined controller"
mkdir -p "$SPACED/job"
cp "$CUSTOM/job/"memory.* "$SPACED/job/"
ESCAPED=${SPACED// /\\040}
printf '29 23 0:26 /delegated %s rw - cgroup cgroup rw,memory\n' "$ESCAPED" > "$MOUNTINFO"
assert_memory 1536
# shellcheck disable=SC2329
free_memory() { echo 3072; }
[[ $(calc_xmx 512 32768) == 1536 ]]
# shellcheck disable=SC2329
free_memory() { echo 256; }
if (calc_xmx 512 32768); then
    echo 'heap selection accepted memory below its minimum' >&2
    exit 1
fi
free_memory() { echo 100000; }
[[ $(calc_xmx 512 32768) == 32768 ]]
printf 'cgroup memory sizing checks passed\n'
