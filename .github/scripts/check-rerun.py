#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check the failed run again immediately before an automatic retry."""

import argparse
import json
import os
import re
import subprocess
from urllib.parse import urlencode


def api(path):
    result = subprocess.run(
        ["gh", "api", "--method", "GET", path], check=True,
        capture_output=True, timeout=30,
    )
    return json.loads(result.stdout)


def decide(repository, run_id, expected_attempt, max_reruns, fetch=api):
    run = fetch(f"repos/{repository}/actions/runs/{run_id}")
    if run.get("path") != ".github/workflows/server-ci.yml":
        return "skip", "only Server CI required jobs automatically retry", None
    if (run.get("status") != "completed" or run.get("conclusion") != "failure"
            or run.get("run_attempt") != expected_attempt):
        return "skip", "source run changed or is no longer a completed failure", None
    if expected_attempt > max_reruns:
        return "skip", "retry limit reached", None
    sha = run.get("head_sha")
    if not sha:
        return "skip", "missing head SHA", None
    if run.get("event") == "pull_request":
        return "skip", "PR automatic retry paused: CI artifacts cannot independently prove the tested code", None
    if run.get("event") == "push":
        if (run.get("repository", {}).get("full_name") != repository
                or run.get("head_repository", {}).get("full_name") != repository):
            return "skip", "push source repository differs", None
        branch = run.get("head_branch")
        if not branch:
            return "skip", "missing push branch", None
        # Read from the workflow repository, never a similarly named fork branch.
        commits = fetch(f"repos/{repository}/commits?" + urlencode({"sha": branch, "per_page": 1}))
        if not commits or commits[0].get("sha") != sha:
            return "skip", "push branch head moved", None
        # A native job rerun includes its dependent jobs. Prefer a failed producer
        # over its failed reporter; never select the advisory branch of Server CI.
        targets = []
        page = 1
        while True:
            jobs = fetch(f"repos/{repository}/actions/runs/{run_id}/attempts/{expected_attempt}/jobs"
                         f"?per_page=100&page={page}")["jobs"]
            for job in jobs:
                if job.get("status") != "completed" or job.get("conclusion") not in {"failure", "timed_out"}:
                    continue
                name = job.get("name", "")
                if name == "check-license":
                    priority = 0
                elif name == "memory-plan":
                    priority = 1
                elif name == "memory-java-runtime / read-version":
                    priority = 2
                elif re.fullmatch(r"build-server / server_memory \(Java [1-9][0-9]*(?:\.[0-9]+)*\)", name):
                    priority = 3
                elif name == "Server memory tests":
                    priority = 4
                else:
                    continue
                # Only failed reporters retry themselves; a timed-out producer
                # must be retried before its dependent failure can recover.
                if priority == 4 and job.get("conclusion") != "failure":
                    continue
                if job["run_id"] != run_id or job["head_sha"] != sha:
                    return "skip", "job does not match source run", None
                job_id = job["id"]
                if not isinstance(job_id, int) or job_id <= 0:
                    return "skip", "invalid job ID", None
                targets.append((priority, job_id))
            if len(jobs) < 100:
                break
            page += 1
        if targets:
            return "rerun", "failed required job and its dependencies", min(targets)[1]
        return "skip", "no failed required job in this attempt", None
    return "skip", "unsupported source event", None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--run-attempt", required=True, type=int)
    parser.add_argument("--max-reruns", required=True, type=int)
    args = parser.parse_args()
    try:
        action, reason, job_id = decide(args.repository, args.run_id, args.run_attempt, args.max_reruns)
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError):
        action, reason, job_id = "skip", "unable to verify current run and source metadata", None
    print(f"{action}: {reason}")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"action={action}\nreason={reason}\njob_id={job_id or ''}\n")


if __name__ == "__main__":
    main()
