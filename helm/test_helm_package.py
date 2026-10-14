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

"""Validate the artifact shipped to Helm, beyond source template tests."""

from pathlib import Path
import os
import shutil
import subprocess
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm" / "hugegraph"
PACKAGE_BUDGET = 512 * 1024


class HelmPackageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise RuntimeError("Helm is required for the packaged chart regression")
        temporary = tempfile.TemporaryDirectory(prefix="hugegraph-chart-")
        cls.addClassCleanup(temporary.cleanup)
        cls.directory = Path(temporary.name)
        subprocess.run(["helm", "package", str(CHART), "--destination", str(cls.directory)],
                       check=True, capture_output=True, text=True)
        packages = list(cls.directory.glob("*.tgz"))
        if len(packages) != 1:
            raise AssertionError("Expected exactly one packaged chart")
        cls.package = packages[0]
        with tarfile.open(cls.package, "r:gz") as archive:
            cls.files = {}
            for member in archive.getmembers():
                if member.isfile():
                    name = member.name.removeprefix("hugegraph/")
                    cls.files[name] = archive.extractfile(member).read()

    def test_archive_budget(self):
        self.assertLess(self.package.stat().st_size, PACKAGE_BUDGET,
                        "Chart exceeds its package budget; check non-runtime assets")

    def test_no_contributor_or_binary_documentation_files(self):
        excluded = ("images/", "tests/", "scripts/", "testdata/")
        leaked = [name for name in self.files
                  if name.startswith(excluded) or name.endswith(".tgz")]
        self.assertEqual([], leaked,
                         "Development assets must not enter the Helm release payload")

    def test_required_configuration_is_preserved(self):
        required = ("Chart.yaml", "README.md", "values.yaml", "values.schema.json",
                    "values-single.yaml", "values-cluster.yaml")
        for name in required:
            with self.subTest(file=name):
                self.assertIn(name, self.files)
                # Helm normalizes Chart.yaml and values.yaml while packaging.
                if name not in ("Chart.yaml", "values.yaml"):
                    self.assertEqual((CHART / name).read_bytes(), self.files[name])

    def test_every_runtime_template_is_preserved(self):
        for source in (CHART / "templates").rglob("*"):
            if source.is_file():
                name = source.relative_to(CHART).as_posix()
                with self.subTest(template=name):
                    self.assertIn(name, self.files)
                    self.assertEqual(source.read_bytes(), self.files[name])

    def test_packaged_chart_renders_each_preset(self):
        for preset in (None, "values-single.yaml", "values-cluster.yaml"):
            with self.subTest(preset=preset or "default"):
                command = ["helm", "template", "package-check", str(self.package)]
                if preset:
                    # Use the values shipped inside the archive, not the source tree.
                    values = self.directory / preset
                    values.write_bytes(self.files[preset])
                    command.extend(["--values", str(values)])
                rendered = subprocess.run(command, check=True, capture_output=True)
                self.assertTrue(b"kind: StatefulSet" in rendered.stdout,
                                "Packaged chart did not render a StatefulSet")
                self.assertTrue(b"kind: Deployment" in rendered.stdout,
                                "Packaged chart did not render a Deployment")

    def test_service_identity_requires_image_entrypoint_mappings(self):
        command = ["helm", "template", "identity-check", str(self.package),
                   "--show-only", "templates/server-deployment.yaml",
                   "--set", "server.auth.enabled=false"]
        rendered = subprocess.run(command, check=True, capture_output=True, text=True).stdout
        # Read the literal shell block without adding a YAML dependency.
        lines = rendered.splitlines()
        start = lines.index("          args:") + 2
        shell = []
        for line in lines[start:]:
            if line.strip() and not line.startswith("              "):
                break
            shell.append(line[14:])
        script = "\n".join(shell) + "\n"
        self.assertTrue(script.startswith("set -euo pipefail\n"))
        original = "server.service_id=DEFAULT\nserver.graphspace=DEFAULT\nusePD=false\n"
        old = "#!/bin/bash\nprintf launched > launched\n"
        current = (ROOT / "hugegraph-server/hugegraph-dist/docker/docker-entrypoint.sh").read_text()
        # Execute the actual image's validation and mappings, then stop before
        # storage initialization or Java launch. No properties encoder is copied.
        compatible = current.split("# ── Build wait-storage env", 1)[0] + "\nprintf launched > launched\n"
        bare = compatible.replace(
            'set_prop_encoded "server.service_id" "${service_id_encoded}" "${REST_SERVER_CONF}"',
            'set_prop "server.service_id" "${HG_SERVER_SERVICE_ID}" "${REST_SERVER_CONF}"')
        no_unicode = compatible.replace(r" | sed 's/\\ /\\u0020/g'", "")
        self.assertNotEqual(compatible, bare)
        self.assertNotEqual(compatible, no_unicode)
        comment_only = old + "\n".join("# " + line for line in current.splitlines()) + "\n"
        with tempfile.TemporaryDirectory(prefix="hugegraph-identity-wrapper-") as temporary:
            directory = Path(temporary)
            (directory / "conf/graphs").mkdir(parents=True)
            (directory / "conf/graphs/hugegraph.properties").write_text("backend=hstore\n")
            # The image mapping uses GNU sed. Reuse it when running on macOS;
            # Linux CI already provides it as sed.
            executable_path = os.environ["PATH"]
            if shutil.which("gsed"):
                (directory / "bin").mkdir()
                (directory / "bin/sed").symlink_to(shutil.which("gsed"))
                executable_path = str(directory / "bin") + os.pathsep + executable_path
            config = directory / "conf/rest-server.properties"
            entrypoint = directory / "docker-entrypoint.sh"
            launcher = directory / "wrapper.sh"
            launcher.write_text(script)
            reader_classpath = os.environ.get("HG_CONFIG_TEST_CLASSPATH")
            if reader_classpath:
                (directory / "reader").mkdir()
                subprocess.run(["javac", "-cp", reader_classpath, "-d", str(directory / "reader"),
                                str(ROOT / "hugegraph-server/hugegraph-dist/docker/test/ConfigReadback.java")],
                               check=True, capture_output=True, text=True, timeout=30)
                reader_classpath = str(directory / "reader") + os.pathsep + reader_classpath
            else:
                print("SKIP: native service readback (HG_CONFIG_TEST_CLASSPATH is unset); "
                      "serialized encoder contract is checked")
            # The real bare encoder serializes a space as backslash-space,
            # which the existing HugeConfig contract preserves incorrectly.
            for image in (bare, no_unicode):
                config.write_text(original)
                entrypoint.write_text(image)
                direct = subprocess.run(["bash", str(entrypoint)], cwd=directory,
                                        env={"PATH": executable_path, "HG_SERVER_SERVICE_ID": "ops team"},
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(0, direct.returncode, direct.stderr)
                self.assertIn(r"server.service_id=ops\ team" + "\n", config.read_text())
                if reader_classpath:
                    readback = subprocess.run(["java", "-cp", reader_classpath, "ConfigReadback",
                                               str(config), "ops team"], capture_output=True, text=True, timeout=10)
                    self.assertNotEqual(0, readback.returncode)
                    self.assertIn("but loaded [ops\\ team]", readback.stderr)
                    print(next(line for line in readback.stderr.splitlines() if "but loaded" in line))
            for name, image, identity, supported in (
                    ("old service", old, {"HG_SERVER_SERVICE_ID": "analytics"}, False),
                    ("old graph space", old, {"HG_SERVER_GRAPHSPACE": "team_a"}, False),
                    ("comment-only service", comment_only, {"HG_SERVER_SERVICE_ID": "analytics"}, False),
                    ("comment-only graph space", comment_only, {"HG_SERVER_GRAPHSPACE": "team_a"}, False),
                    ("bare service mapping", bare, {"HG_SERVER_SERVICE_ID": "ops team"}, False),
                    ("encoded mapping without Unicode spaces", no_unicode,
                     {"HG_SERVER_SERVICE_ID": "ops team"}, False),
                    ("current mappings", compatible,
                     {"HG_SERVER_SERVICE_ID": "analytics", "HG_SERVER_GRAPHSPACE": "team_a"}, True),
                    ("current mapping with spaces", compatible,
                     {"HG_SERVER_SERVICE_ID": "ops team", "HG_SERVER_GRAPHSPACE": "team_a"}, True),
                    ("current mapping with boundary spaces", compatible,
                     {"HG_SERVER_SERVICE_ID": " ops team ", "HG_SERVER_GRAPHSPACE": "team_a"}, True),
                    ("default old image", old, {}, True)):
                with self.subTest(case=name):
                    config.write_text(original)
                    entrypoint.write_text(image)
                    entrypoint.chmod(0o700)
                    (directory / "launched").unlink(missing_ok=True)
                    environment = {"PATH": executable_path,
                                   "HG_SERVER_PD_PEERS": "fixture:8686",
                                   "HG_SERVER_URLS_TO_PD": "http://fixture:8080", **identity}
                    run = subprocess.run(["bash", str(launcher)], cwd=directory, env=environment,
                                         capture_output=True, text=True, timeout=10)
                    if supported:
                        self.assertEqual(0, run.returncode, run.stderr)
                        self.assertTrue((directory / "launched").exists())
                        self.assertIn("usePD=true\n", config.read_text())
                        if identity:
                            service_id = identity["HG_SERVER_SERVICE_ID"]
                            encoded = service_id.replace(" ", r"\u0020")
                            self.assertIn("server.service_id=" + encoded + "\n", config.read_text())
                            if reader_classpath:
                                readback = subprocess.run(["java", "-cp", reader_classpath, "ConfigReadback",
                                                           str(config), service_id],
                                                          capture_output=True, text=True, timeout=10)
                                self.assertEqual(0, readback.returncode, readback.stderr)
                                print(readback.stdout.strip())
                            self.assertIn("server.graphspace=team_a\n", config.read_text())
                    else:
                        self.assertNotEqual(0, run.returncode,
                                            "Unsupported image launched=" +
                                            str((directory / "launched").exists()) +
                                            "; config changed=" + str(config.read_text() != original))
                        self.assertIn("compatible Server image", run.stderr)
                        self.assertEqual(original, config.read_text(), "Rejected image changed config")
                        self.assertFalse((directory / "launched").exists(), "Rejected image was launched")


if __name__ == "__main__":
    unittest.main()
