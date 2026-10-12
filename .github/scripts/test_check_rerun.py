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

"""Behavioral tests for push retry freshness, without credentials or network."""
import importlib.util
from pathlib import Path
import unittest
import os
import subprocess
import tempfile
import json
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("check_rerun", Path(__file__).with_name("check-rerun.py"))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class FreshnessTest(unittest.TestCase):
    def setUp(self):
        self.repository = "apache/hugegraph"
        self.run = {"status": "completed", "conclusion": "failure", "run_attempt": 1,
                    "path": ".github/workflows/server-ci.yml", "event": "push", "head_sha": "old", "head_branch": "release/test",
                    "repository": {"full_name": self.repository},
                    "head_repository": {"full_name": self.repository}}
        self.current_head = "old"
        self.calls = []
        self.jobs = [self.job(117, "build-server / server_memory (Java 17)"),
                     self.job(118, "Server memory tests")]

    def job(self, job_id, name, conclusion="failure"):
        return {"id": job_id, "name": name, "status": "completed", "conclusion": conclusion,
                "run_id": 42, "head_sha": "old"}

    def fetch(self, path):
        self.calls.append(path)
        if path == f"repos/{self.repository}/actions/runs/42":
            return self.run
        if path == f"repos/{self.repository}/commits?sha=release%2Ftest&per_page=1":
            return [{"sha": self.current_head}] if self.current_head else []
        if path.startswith(f"repos/{self.repository}/actions/runs/42/attempts/"):
            page = int(path.rsplit("page=", 1)[1])
            return {"jobs": self.jobs[(page - 1) * 100:page * 100]}
        raise AssertionError("Unexpected lookup: " + path)

    def decide(self):
        return checker.decide(self.repository, 42, 1, 2, self.fetch)[0]

    def test_only_server_required_jobs_retry(self):
        for path in [".github/workflows/server-memory-ci.yml", ".github/workflows/licence-checker.yml",
                     ".github/workflows/codeql-analysis.yml", ".github/workflows/server-compatibility-ci.yml"]:
            self.run["path"] = path
            self.assertEqual("skip", self.decide())
        self.run["path"] = ".github/workflows/server-ci.yml"
        self.assertEqual("rerun", self.decide())

    def test_failed_producer_is_selected_before_reporter(self):
        for name in ["check-license", "memory-plan", "memory-java-runtime / read-version",
                     "build-server / server_memory (Java 17)"]:
            with self.subTest(name=name):
                self.jobs = [self.job(118, "Server memory tests"), self.job(117, name)]
                self.assertEqual(117, checker.decide(self.repository, 42, 1, 2, self.fetch)[2])
        self.jobs = [self.job(118, "Server memory tests")]
        self.assertEqual(118, checker.decide(self.repository, 42, 1, 2, self.fetch)[2])

    def test_timed_out_producer_is_retried_only_inside_failed_run(self):
        self.jobs = [self.job(118, "Server memory tests"),
                     self.job(117, "build-server / server_memory (Java 17)", "timed_out")]
        self.assertEqual(117, checker.decide(self.repository, 42, 1, 2, self.fetch)[2])
        self.run["conclusion"] = "timed_out"
        self.assertEqual("skip", self.decide())

    def test_timed_out_reporter_alone_is_not_retried(self):
        self.jobs = [self.job(118, "Server memory tests", "timed_out")]
        self.assertEqual("skip", self.decide())

    def test_advisory_success_and_non_failure_jobs_are_not_retried(self):
        for name in ["plan", "java-runtime / read-version", "server-rocksdb / server_rocksdb (Java 17)",
                     "pd_store / struct", "affected-module-tests", "build-server / server_memory (Java 17) / extra"]:
            self.jobs = [self.job(118, name)]
            self.assertEqual("skip", self.decide())
        for conclusion in ["success", "cancelled", "skipped", None]:
            self.jobs = [self.job(118, "Server memory tests", conclusion)]
            self.assertEqual("skip", self.decide())

    def test_job_selection_paginates_the_exact_attempt(self):
        self.jobs = [self.job(i + 1, "advisory") for i in range(100)] + [self.job(117, "memory-plan")]
        self.run["run_attempt"] = 2
        self.assertEqual(117, checker.decide(self.repository, 42, 2, 2, self.fetch)[2])
        self.assertIn(f"repos/{self.repository}/actions/runs/42/attempts/2/jobs?per_page=100&page=2", self.calls)
        self.assertFalse(any("/attempts/1/" in path for path in self.calls))

    def test_job_identity_and_metadata_must_match(self):
        for key, value in [("run_id", 99), ("head_sha", "different"), ("id", "117"), ("id", -1)]:
            with self.subTest(key=key):
                self.jobs = [dict(self.job(117, "memory-plan"), **{key: value})]
                self.assertEqual("skip", self.decide())

    def test_workflow_posts_native_rerun_for_checker_selected_job(self):
        workflow = Path(__file__).resolve().parents[1] / "workflows/rerun-ci.yml"
        source = workflow.read_text()
        self.assertIn('      - "HugeGraph-Server CI"', source)
        self.assertIn("github.event.workflow_run.event == 'push'", source)
        self.assertIn("ref: ${{ github.sha }}", source)
        command = source.split("      - name: Rerun the required job and its dependencies", 1)[1]
        command = command.split("        run: |\n", 1)[1].strip()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output"
            decide = checker.decide
            with patch.dict(os.environ, {"GITHUB_OUTPUT": str(output)}), patch(
                    "sys.argv", ["check-rerun.py", "--repository", self.repository,
                                 "--run-id", "42", "--run-attempt", "1", "--max-reruns", "2"]), patch.object(
                    checker, "decide", side_effect=lambda *args: decide(*args, fetch=self.fetch)):
                checker.main()
            selected = dict(line.split("=", 1) for line in output.read_text().splitlines())
            self.assertEqual("rerun", selected["action"])
            command = command.replace("${{ steps.freshness.outputs.job_id }}", selected["job_id"])
            gh = root / "gh"
            gh.write_text('#!/usr/bin/env python3\nimport json, os, sys\n'
                          'open(os.environ["ARGS_PATH"], "w").write(json.dumps(sys.argv[1:]))\n')
            gh.chmod(0o700)
            args_path = root / "args.json"
            subprocess.run(["bash", "-eu", "-c", command], check=True,
                           env={**os.environ, "GH_REPO": self.repository, "ARGS_PATH": str(args_path),
                                "PATH": str(root) + os.pathsep + os.environ["PATH"]})
            self.assertEqual(["api", "--method", "POST", f"repos/{self.repository}/actions/jobs/117/rerun"],
                             json.loads(args_path.read_text()))

    def test_api_failure_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            with patch.dict(os.environ, {"GITHUB_OUTPUT": str(output)}), patch(
                    "sys.argv", ["check-rerun.py", "--repository", self.repository,
                                 "--run-id", "42", "--run-attempt", "1", "--max-reruns", "2"]), patch.object(
                    checker, "decide", side_effect=subprocess.CalledProcessError(1, "gh")):
                checker.main()
            self.assertIn("action=skip\n", output.read_text())

    def test_pr_is_paused_without_reading_artifacts_or_candidates(self):
        self.run.update(event="pull_request", head_repository={"full_name": "alice/hugegraph"})
        self.assertEqual("skip", self.decide())
        self.assertEqual([f"repos/{self.repository}/actions/runs/42"], self.calls)

    def test_changed_attempt(self):
        self.run["run_attempt"] = 2
        self.assertEqual("skip", self.decide())

    def test_current_run_must_remain_failed_and_completed(self):
        for key, value in (("status", "in_progress"), ("conclusion", "success"),
                           ("conclusion", "cancelled")):
            with self.subTest(key=key, value=value):
                old = self.run[key]
                self.run[key] = value
                self.assertEqual("skip", self.decide())
                self.run[key] = old

    def test_retry_limit(self):
        self.run["run_attempt"] = 3
        self.assertEqual("skip", checker.decide(self.repository, 42, 3, 2, self.fetch)[0])
        self.run["run_attempt"] = 2
        self.assertEqual("rerun", checker.decide(self.repository, 42, 2, 2, self.fetch)[0])

    def test_push_checks_repository_and_encodes_branch(self):
        self.assertEqual("rerun", self.decide())
        self.assertIn(f"repos/{self.repository}/commits?sha=release%2Ftest&per_page=1", self.calls)
        self.current_head = "new"
        self.assertEqual("skip", self.decide())
        self.current_head = None
        self.assertEqual("skip", self.decide())

    def test_push_rejects_other_repository(self):
        for field in ["repository", "head_repository"]:
            with self.subTest(field=field):
                self.run[field] = {"full_name": "alice/hugegraph"}
                self.assertEqual("skip", self.decide())
                self.run[field] = {"full_name": self.repository}

    def test_missing_metadata_and_unsupported_event(self):
        for field in ["head_sha", "head_branch", "repository", "head_repository"]:
            old = self.run.pop(field)
            with self.subTest(field=field):
                self.assertEqual("skip", self.decide())
            self.run[field] = old
        self.run["event"] = "workflow_dispatch"
        self.assertEqual("skip", self.decide())


if __name__ == "__main__":
    unittest.main()
