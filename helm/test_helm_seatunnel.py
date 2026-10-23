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

"""Check the optional engine's rendered topology, connectivity and release payload."""

from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parent / "hugegraph"


def matches(selector, labels):
    return all(labels.get(key) == value for key, value in selector.items())


def engine_resources(documents, kind=None):
    return [doc for doc in documents if (kind is None or doc["kind"] == kind)
            and doc["metadata"].get("labels", {}).get("app.kubernetes.io/part-of") == "seatunnel"]


def pod(workload):
    return workload["spec"]["template"]


class SeaTunnelChartTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise RuntimeError("Helm is required for the SeaTunnel chart regression")

    def render(self, seatunnel=None, chart=CHART, namespace="default", **values):
        if seatunnel is not None:
            values["seatunnel"] = {"enabled": True, **seatunnel}
        with tempfile.TemporaryDirectory(prefix="seatunnel-values-") as directory:
            configuration = Path(directory) / "values.yaml"
            configuration.write_text(yaml.safe_dump(values))
            result = subprocess.run(["helm", "template", "fixture", str(chart),
                                     "--namespace", namespace, "-f", str(configuration)],
                                    capture_output=True, text=True)
        return result

    def documents(self, seatunnel=None, **values):
        result = self.render(seatunnel, **values)
        self.assertEqual(0, result.returncode, result.stderr)
        return [doc for doc in yaml.safe_load_all(result.stdout) if doc]

    def test_disabled_has_no_engine_resources_or_child_validation(self):
        documents = self.documents()
        self.assertEqual([], engine_resources(documents))
        self.assertFalse(any("seatunnel" in doc["metadata"]["name"] for doc in documents))
        # Disabled dependencies must not validate inactive engine settings.
        self.assertEqual(0, self.render({"enabled": False, "mode": "unused"}).returncode)

    def test_standalone_is_one_hybrid_with_usable_configuration(self):
        documents = self.documents({})
        workloads = engine_resources(documents, "Deployment")
        self.assertEqual(1, len(workloads))
        master = workloads[0]
        self.assertEqual(1, master["spec"]["replicas"])
        self.assertEqual("Recreate", master["spec"]["strategy"]["type"])
        container = pod(master)["spec"]["containers"][0]
        # SeaTunnel 3.0.0 selects the hybrid role when -r is omitted; its Java
        # entrypoint rejects an explicit -r master_and_worker.
        self.assertTrue(container["command"][-1].rstrip().endswith("/seatunnel-cluster.sh"))
        self.assertNotIn(" -r ", container["command"][-1])
        self.assertFalse(pod(master)["spec"]["automountServiceAccountToken"])
        mounts = {mount["subPath"] for mount in container["volumeMounts"]}
        self.assertTrue({"hazelcast.yaml", "jvm_options", "seatunnel.yaml"} <= mounts)
        config = engine_resources(documents, "ConfigMap")[0]["data"]
        engine = yaml.safe_load(config["seatunnel.yaml"])["seatunnel"]["engine"]
        self.assertEqual(0, engine["backup-count"])
        self.assertTrue(engine["http"]["enable-http"])
        self.assertFalse(engine["http"]["enable-dynamic-port"])

    def test_cluster_roles_and_mode_switch_preserve_master_identity(self):
        standalone = self.documents({})
        cluster = self.documents({"mode": "cluster"})
        workloads = engine_resources(cluster, "Deployment")
        self.assertEqual({"master", "worker"},
                         {doc["metadata"]["labels"]["app.kubernetes.io/component"] for doc in workloads})
        for workload in workloads:
            role = workload["metadata"]["labels"]["app.kubernetes.io/component"]
            self.assertEqual(2, workload["spec"]["replicas"])
            self.assertTrue(pod(workload)["spec"]["containers"][0]["command"][-1].rstrip().endswith("-r " + role))
        first = engine_resources(standalone, "Deployment")[0]
        master = next(doc for doc in workloads if doc["metadata"]["name"] == first["metadata"]["name"])
        self.assertEqual(first["spec"]["selector"], master["spec"]["selector"])
        services = lambda docs: {doc["metadata"]["name"]: doc["spec"]["selector"]
                                 for doc in engine_resources(docs, "Service")}
        self.assertEqual(services(standalone), services(cluster))
        self.assertNotEqual(pod(first)["metadata"]["annotations"]["checksum/config"],
                            pod(master)["metadata"]["annotations"]["checksum/config"])
        data = engine_resources(cluster, "ConfigMap")[0]["data"]
        self.assertEqual(1, yaml.safe_load(data["seatunnel.yaml"])["seatunnel"]["engine"]["backup-count"])

    def test_peer_service_selects_both_roles_and_only_its_release(self):
        documents = self.documents({"mode": "cluster", "fullnameOverride": "transfer",
                                    "clusterDomain": "corp.internal"}, namespace="graphs")
        service = next(doc for doc in engine_resources(documents, "Service")
                       if doc["spec"].get("clusterIP") == "None")
        self.assertEqual("transfer", service["metadata"]["name"])
        self.assertTrue(service["spec"]["publishNotReadyAddresses"])
        selector = service["spec"]["selector"]
        workloads = [doc for doc in documents if doc["kind"] in ("Deployment", "StatefulSet")]
        selected = [doc for doc in workloads if matches(selector, pod(doc)["metadata"]["labels"])]
        self.assertEqual(2, len(selected))
        self.assertFalse(matches(selector, {**pod(selected[0])["metadata"]["labels"],
                                            "app.kubernetes.io/instance": "another-release"}))
        config = engine_resources(documents, "ConfigMap")[0]["data"]
        for filename in ("hazelcast.yaml", "hazelcast-master.yaml", "hazelcast-worker.yaml"):
            discovery = yaml.safe_load(config[filename])["hazelcast"]["network"]["join"]["kubernetes"]
            self.assertEqual("transfer.graphs.svc.corp.internal", discovery["service-dns"])
        self.assertIn("transfer.graphs.svc.corp.internal:5801", config["hazelcast-client.yaml"])

    def test_runtime_overrides_and_config_changes_reach_pods(self):
        digest = "sha256:" + "a" * 64
        values = {"image": {"registry": "team/seatunnel", "digest": digest, "pullSecret": "registry-auth"},
                  "env": [{"name": "GRAPH_PASSWORD", "valueFrom": {
                      "secretKeyRef": {"name": "graph-auth", "key": "password"}}}],
                  "service": {"port": 9080}, "engineConfig": {"checkpoint": {"interval": 10000}}}
        documents = self.documents(values, **{"global": {"imageRegistry": "registry.example.com"}})
        master = engine_resources(documents, "Deployment")[0]
        spec = pod(master)["spec"]
        container = spec["containers"][0]
        self.assertEqual("registry.example.com/team/seatunnel@" + digest, container["image"])
        self.assertEqual([{"name": "registry-auth"}], spec["imagePullSecrets"])
        self.assertEqual(values["env"], container["env"])
        data = engine_resources(documents, "ConfigMap")[0]["data"]
        engine = yaml.safe_load(data["seatunnel.yaml"])["seatunnel"]["engine"]
        self.assertEqual(9080, engine["http"]["port"])
        self.assertEqual(10000, engine["checkpoint"]["interval"])
        previous = engine_resources(self.documents({}), "Deployment")[0]
        self.assertNotEqual(pod(previous)["metadata"]["annotations"]["checksum/config"],
                            pod(master)["metadata"]["annotations"]["checksum/config"])
        documents = self.documents({"image": {"registry": "private.example.com/engine", "tag": "tested"}},
                                   **{"global": {"imageRegistry": "registry.example.com"}})
        image = pod(engine_resources(documents, "Deployment")[0])["spec"]["containers"][0]["image"]
        self.assertEqual("private.example.com/engine:tested", image)

    def test_invalid_enabled_values_fail_before_install(self):
        for values in ({"mode": "invalid"}, {"master": {"replicas": 2}},
                       {"mode": "cluster", "master": {"replicas": 0}},
                       {"mode": "cluster", "worker": {"replicas": 0}},
                       {"service": {"port": 5801}},
                       {"configMap": {"create": False, "existingConfigMapName": ""}}):
            with self.subTest(values=values):
                self.assertNotEqual(0, self.render(values).returncode)
        documents = self.documents({"configMap": {"create": False, "existingConfigMapName": "engine-config"}})
        self.assertEqual([], engine_resources(documents, "ConfigMap"))
        spec = pod(engine_resources(documents, "Deployment")[0])["spec"]
        self.assertEqual("engine-config", spec["volumes"][0]["configMap"]["name"])
        documents = self.documents({"mode": "cluster", "master": {"strategy": {"type": "Recreate"}},
                                    "worker": {"strategy": {"type": "Recreate"}}})
        for workload in engine_resources(documents, "Deployment"):
            self.assertEqual({"type": "Recreate"}, workload["spec"]["strategy"])

    def test_strict_policies_allow_peers_server_dns_and_native_extensions(self):
        extra_in = {"from": [{"ipBlock": {"cidr": "10.10.0.0/16"}}], "ports": [{"port": 9080}]}
        extra_out = {"to": [{"ipBlock": {"cidr": "10.20.0.0/16"}}], "ports": [{"port": 9099}]}
        documents = self.documents({"mode": "cluster", "service": {"port": 9080}},
                                   mode="strict", server={"port": 8182},
                                   networkPolicy={"seatunnel": {"extraIngress": [extra_in],
                                                                "extraEgress": [extra_out]}})
        policies = [doc for doc in documents if doc["kind"] == "NetworkPolicy"]
        engine = next(doc["spec"] for doc in policies if doc["metadata"]["name"].endswith("-seatunnel"))
        server = next(doc["spec"] for doc in policies if doc["metadata"]["name"].endswith("-server"))
        labels = pod(engine_resources(documents, "Deployment")[0])["metadata"]["labels"]
        self.assertTrue(matches(engine["podSelector"]["matchLabels"], labels))
        self.assertIn(extra_in, engine["ingress"])
        self.assertIn(extra_out, engine["egress"])
        def allows(rules, direction, selector, port):
            return any(peer.get("podSelector", {}).get("matchLabels") == selector
                       and any(entry["port"] == port for entry in rule.get("ports", []))
                       for rule in rules for peer in rule.get(direction, []))
        engine_selector = engine["podSelector"]["matchLabels"]
        self.assertEqual("fixture", engine_selector["app.kubernetes.io/instance"])
        self.assertTrue(allows(server["ingress"], "from", engine_selector, 8182))
        self.assertTrue(allows(engine["egress"], "to", server["podSelector"]["matchLabels"], 8182))
        for direction, peer_key in (("ingress", "from"), ("egress", "to")):
            self.assertTrue(allows(engine[direction], peer_key, engine_selector, 5801))
            self.assertTrue(allows(engine[direction], peer_key, engine_selector, 9080))
        dns = [port for rule in engine["egress"] if not rule.get("to") for port in rule["ports"]]
        self.assertTrue({("UDP", 53), ("TCP", 53)} <= {(p["protocol"], p["port"]) for p in dns})

    def test_package_preserves_dependency_configuration_and_all_modes(self):
        with tempfile.TemporaryDirectory(prefix="seatunnel-package-") as directory:
            subprocess.run(["helm", "package", str(CHART), "--destination", directory],
                           check=True, capture_output=True, text=True)
            package = next(Path(directory).glob("*.tgz"))
            with tarfile.open(package, "r:gz") as archive:
                parent = yaml.safe_load(archive.extractfile("hugegraph-helm/Chart.yaml"))
                dependency = next(d for d in parent["dependencies"] if d.get("alias") == "seatunnel")
                self.assertEqual("seatunnel.enabled", dependency["condition"])
                filename = f'{dependency["name"]}-{dependency["version"]}.tgz'
                child = CHART / "charts" / filename
                # HugeGraph ships one dependency archive; canonical templates
                # belong to the SeaTunnel repository rather than a source copy.
                self.assertEqual([child], list((CHART / "charts").iterdir()))
                with tarfile.open(child, "r:gz") as source:
                    for member in source.getmembers():
                        relative = member.name.split("/", 1)[-1]
                        if relative == "values.schema.json" or relative.startswith(("templates/", "conf/")):
                            name = "hugegraph-helm/charts/" + member.name
                            self.assertEqual(source.extractfile(member).read(), archive.extractfile(name).read(), name)
            self.assertEqual([], engine_resources(self.documents(chart=package)))
            for mode, count in (("standalone", 1), ("cluster", 2)):
                documents = self.documents({"mode": mode}, chart=package)
                self.assertEqual(count, len(engine_resources(documents, "Deployment")))


if __name__ == "__main__":
    unittest.main()
