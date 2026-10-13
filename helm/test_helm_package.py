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
                    "values-single.yaml", "values-cluster.yaml", "values-strict.yaml",
                    "docs/operations.md", "examples/README.md")
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
        for preset in (None, "values-single.yaml", "values-cluster.yaml", "values-strict.yaml"):
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


if __name__ == "__main__":
    unittest.main()
