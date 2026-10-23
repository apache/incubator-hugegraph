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

"""Check native hybrid deployment, release isolation, connectivity and packaging."""

from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parent / "hugegraph"
HELM_TEMPLATE_TIMEOUT_SECONDS = 120
HELM_PACKAGE_TIMEOUT_SECONDS = 300


def matches(selector, labels):
    return all(labels.get(key) == value for key, value in selector.items())


def engine_resources(documents, kind=None):
    return [doc for doc in documents if (kind is None or doc["kind"] == kind)
            and doc["metadata"].get("labels", {}).get("app.kubernetes.io/component") == "seatunnel"]


def pod(workload):
    return workload["spec"]["template"]


class SeaTunnelChartTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise RuntimeError("Helm is required for the SeaTunnel integration regression")

    def run_helm(self, arguments, timeout, check=False):
        command = ["helm", *arguments]
        try:
            return subprocess.run(command, check=check, capture_output=True,
                                  text=True, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            self.fail(f"{' '.join(command)} timed out after {timeout} seconds: {error}")

    def render(self, seatunnel=None, chart=CHART, namespace="default", **values):
        if seatunnel is not None:
            values["seatunnel"] = {"enabled": True, **seatunnel}
        with tempfile.TemporaryDirectory(prefix="seatunnel-values-") as directory:
            configuration = Path(directory) / "values.yaml"
            configuration.write_text(yaml.safe_dump(values))
            result = self.run_helm(["template", "fixture", str(chart),
                                    "--namespace", namespace, "-f", str(configuration)],
                                   HELM_TEMPLATE_TIMEOUT_SECONDS)
        return result

    def documents(self, seatunnel=None, chart=CHART, namespace="default", **values):
        result = self.render(seatunnel, chart=chart, namespace=namespace, **values)
        self.assertEqual(0, result.returncode, result.stderr)
        return [doc for doc in yaml.safe_load_all(result.stdout) if doc]

    def test_disabled_has_no_engine_resources(self):
        documents = self.documents()
        self.assertEqual([], engine_resources(documents))
        self.assertFalse(any("seatunnel" in doc["metadata"]["name"] for doc in documents))

    def test_modes_share_one_native_hybrid_workload_and_services(self):
        modes = [self.documents({}), self.documents({"mode": "cluster", "replicas": 3})]
        workloads = []
        for documents, replicas, backups in zip(modes, (1, 3), (0, 1)):
            workload, = engine_resources(documents, "StatefulSet")
            workloads.append(workload)
            self.assertEqual([], engine_resources(documents, "Deployment"))
            self.assertEqual(replicas, workload["spec"]["replicas"])
            container, = pod(workload)["spec"]["containers"]
            launch = " ".join(container.get("command", []) + container.get("args", []))
            self.assertIn("/opt/seatunnel/bin/seatunnel-cluster.sh", launch)
            self.assertNotIn(" -r ", launch)
            data = engine_resources(documents, "ConfigMap")[0]["data"]
            engine = yaml.safe_load(data["seatunnel.yaml"])["seatunnel"]["engine"]
            self.assertEqual(backups, engine["backup-count"])
            self.assertTrue(engine["http"]["enable-http"])
            self.assertEqual(8080, engine["http"]["port"])
        self.assertEqual(workloads[0]["metadata"]["name"], workloads[1]["metadata"]["name"])
        self.assertEqual(workloads[0]["spec"]["selector"], workloads[1]["spec"]["selector"])
        services = lambda docs: {doc["metadata"]["name"]: doc["spec"]["selector"]
                                 for doc in engine_resources(docs, "Service")}
        self.assertEqual(services(modes[0]), services(modes[1]))

    def test_dns_discovery_and_services_are_scoped_to_engine_and_release(self):
        documents = self.documents({"mode": "cluster"}, namespace="graphs", fullnameOverride="transfer")
        workload, = engine_resources(documents, "StatefulSet")
        labels = pod(workload)["metadata"]["labels"]
        services = engine_resources(documents, "Service")
        headless, = [service for service in services if service["spec"].get("clusterIP") == "None"]
        api, = [service for service in services if service is not headless]
        self.assertEqual("transfer-seatunnel", workload["metadata"]["name"])
        self.assertEqual(headless["metadata"]["name"], workload["spec"]["serviceName"])
        self.assertTrue(headless["spec"]["publishNotReadyAddresses"])
        self.assertIn(8080, [port["port"] for port in api["spec"]["ports"]])
        for service in services:
            selector = service["spec"]["selector"]
            self.assertTrue(matches(selector, labels))
            self.assertFalse(matches(selector, {**labels, "app.kubernetes.io/instance": "other"}))
            for other in documents:
                if other["kind"] in ("Deployment", "StatefulSet") and other is not workload:
                    self.assertFalse(matches(selector, pod(other)["metadata"]["labels"]))
        data = engine_resources(documents, "ConfigMap")[0]["data"]
        discovery = yaml.safe_load(data["hazelcast.yaml"])["hazelcast"]["network"]["join"]["kubernetes"]
        self.assertTrue(discovery["enabled"])
        self.assertTrue(discovery["service-dns"].startswith(headless["metadata"]["name"] + ".graphs.svc"))
        self.assertNotIn("service-name", discovery)
        # DNS discovery otherwise dials Hazelcast's default5701 and creates
        # isolated one-member clusters even while both Pods report Ready.
        member_port = next(port["containerPort"] for port in
                           pod(workload)["spec"]["containers"][0]["ports"]
                           if port["name"] == "hazelcast")
        self.assertEqual(member_port, discovery["service-port"])
        client = yaml.safe_load(data["hazelcast-client.yaml"])["hazelcast-client"]
        self.assertIn(discovery["service-dns"] + ":5801", client["network"]["cluster-members"])
        self.assertFalse(any(doc["kind"] in ("Role", "RoleBinding") for doc in engine_resources(documents)))

    def test_image_secret_environment_and_native_file_overrides_reach_engine(self):
        custom = {"jvm_options": "-Xms128m\n-Xmx384m\n", "log4j2.properties": "status=warn\n"}
        secret_env = {"name": "HG_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "graph-auth", "key": "password"}}}
        documents = self.documents({"image": {"repository": "team/seatunnel", "tag": "tested", "pullPolicy": "Never"},
                                    "env": [secret_env], "config": custom},
                                   **{"global": {"imageRegistry": "registry.example.com"},
                                      "imagePullSecrets": [{"name": "registry-auth"}]})
        workload, = engine_resources(documents, "StatefulSet")
        spec = pod(workload)["spec"]
        container, = spec["containers"]
        self.assertEqual("registry.example.com/team/seatunnel:tested", container["image"])
        self.assertEqual("Never", container["imagePullPolicy"])
        self.assertEqual([{"name": "registry-auth"}], spec["imagePullSecrets"])
        self.assertIn(secret_env, container["env"])
        data = engine_resources(documents, "ConfigMap")[0]["data"]
        mounts = {entry["subPath"] for entry in container["volumeMounts"]}
        for filename, content in custom.items():
            self.assertEqual(content, data[filename])
            self.assertIn(filename, mounts)
        previous, = engine_resources(self.documents({}), "StatefulSet")
        self.assertNotEqual(pod(previous)["metadata"]["annotations"]["checksum/config"],
                            pod(workload)["metadata"]["annotations"]["checksum/config"])

    def test_invalid_enabled_mode_and_cluster_size_fail_before_install(self):
        for values in ({"mode": "invalid"}, {"mode": "cluster", "replicas": 0},
                       {"mode": "cluster", "replicas": 1}):
            with self.subTest(values=values):
                self.assertNotEqual(0, self.render(values).returncode)

    def test_strict_policies_allow_engine_server_dns_and_native_extensions(self):
        extra_in = {"from": [{"ipBlock": {"cidr": "10.10.0.0/16"}}], "ports": [{"port": 8080}]}
        extra_out = {"to": [{"ipBlock": {"cidr": "10.20.0.0/16"}}], "ports": [{"port": 9099}]}
        documents = self.documents({"mode": "cluster"}, mode="strict", server={"port": 8182},
                                   networkPolicy={"seatunnel": {"extraIngress": [extra_in], "extraEgress": [extra_out]}})
        policies = [doc for doc in documents if doc["kind"] == "NetworkPolicy"]
        engine = next(doc["spec"] for doc in policies if doc["metadata"]["name"].endswith("-seatunnel"))
        server = next(doc["spec"] for doc in policies if doc["metadata"]["name"].endswith("-server"))
        workload, = engine_resources(documents, "StatefulSet")
        self.assertTrue(matches(engine["podSelector"]["matchLabels"], pod(workload)["metadata"]["labels"]))
        self.assertIn(extra_in, engine["ingress"])
        self.assertIn(extra_out, engine["egress"])

        def allows(rules, direction, selector, port):
            return any(peer.get("podSelector", {}).get("matchLabels") == selector
                       and any(entry["port"] == port for entry in rule.get("ports", []))
                       for rule in rules for peer in rule.get(direction, []))

        selector = engine["podSelector"]["matchLabels"]
        self.assertTrue(allows(server["ingress"], "from", selector, 8182))
        self.assertTrue(allows(engine["egress"], "to", server["podSelector"]["matchLabels"], 8182))
        for direction, peer_key in (("ingress", "from"), ("egress", "to")):
            for port in (5801, 8080):
                self.assertTrue(allows(engine[direction], peer_key, selector, port))
        dns = [port for rule in engine["egress"] if not rule.get("to") for port in rule["ports"]]
        self.assertTrue({("UDP", 53), ("TCP", 53)} <= {(p["protocol"], p["port"]) for p in dns})

    def test_packaged_chart_has_both_modes_without_seatunnel_dependency(self):
        with tempfile.TemporaryDirectory(prefix="seatunnel-package-") as directory:
            self.run_helm(["package", str(CHART), "--destination", directory],
                          HELM_PACKAGE_TIMEOUT_SECONDS, check=True)
            package = next(Path(directory).glob("*.tgz"))
            with tarfile.open(package, "r:gz") as archive:
                metadata = yaml.safe_load(archive.extractfile("hugegraph-helm/Chart.yaml"))
                self.assertFalse(any("seatunnel" in dependency["name"]
                                     for dependency in metadata.get("dependencies", [])))
                self.assertTrue(any(name.endswith("/templates/seatunnel.yaml") for name in archive.getnames()))
            self.assertEqual([], engine_resources(self.documents(chart=package)))
            for mode, replicas in (("standalone", 1), ("cluster", 2)):
                documents = self.documents({"mode": mode}, chart=package)
                workload, = engine_resources(documents, "StatefulSet")
                self.assertEqual(replicas, workload["spec"]["replicas"])
                source, = engine_resources(self.documents({"mode": mode}), "StatefulSet")
                self.assertEqual(source["spec"], workload["spec"])


if __name__ == "__main__":
    unittest.main()
