/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.k8s;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.space.Service;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSource;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.HTTPGetAction;
import io.fabric8.kubernetes.api.model.HTTPGetActionBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.NamespaceList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class K8sDriver {

    protected static final Logger LOG = Log.logger(K8sDriver.class);

    private static final String DELIMETER = "-";
    private static final String COLON = ":";
    private static final String COMMA = ",";

    private static final String CONTAINER = "container";
    private static final String APP = "app";
    private static final String PORT_SUFFIX = "-port";
    private static final String TCP = "TCP";

    private static final String CLUSTER_IP = "ClusterIP";
    private static final String LOAD_BALANCER = "LoadBalancer";
    private static final String NODE_PORT = "NodePort";
    private static final int HG_PORT = 8080;

    private static final String CPU = "cpu";
    private static final String MEMORY = "memory";
    private static final String CPU_UNIT = "m";
    private static final String MEMORY_UNIT = "G";

    private static final String HEALTH_CHECK_API = "/versions";

    private static final String CA_CONFIG_MAP_NAME = "hg-ca";

    private static final String GRAPH_SPACE = "GRAPH_SPACE";
    private static final String SERVICE_ID = "SERVICE_ID";
    private static final String META_SERVERS = "META_SERVERS";
    private static final String CLUSTER = "CLUSTER";

    private static final String MY_NODE_NAME = "MY_NODE_NAME";
    private static final String MY_POD_IP = "MY_POD_IP";
    private static final String SPEC_NODE_NAME = "spec.nodeName";
    private static final String STATUS_POD_IP = "status.podIP";
    private static final String APP_NAME = "APP_NAME";

    private static final String SERVICE_ACCOUNT_NAME = "hugegraph-user";
    private static final String SERVICE_ACCOUNT = "ServiceAccount";
    private static final String BINDING_API_GROUP = "rbac.authorization.k8s.io";
    private static final String CLUSTER_ROLE = "ClusterRole";
    private static final String CLUSTER_ROLE_NAME = "cluster-admin";
    private static final String BINDING_API_VERSION =
            "rbac.authorization.k8s.io/v1";

    private KubernetesClient client;

    private String oltpImage;
    private String olapImage;
    private String storageImage;

    private CA ca;

    public K8sDriver(String url, String caFile, String clientCaFile,
                     String clientKeyFile) {
        Config config = new ConfigBuilder().withMasterUrl(url)
                                           .withTrustCerts(true)
                                           .withCaCertFile(caFile)
                                           .withClientCertFile(clientCaFile)
                                           .withClientKeyFile(clientKeyFile)
                                           .build();
        this.client = new DefaultKubernetesClient(config);
    }

    public K8sDriver(String url) {
        Config config = new ConfigBuilder().withMasterUrl(url).build();
        this.client = new DefaultKubernetesClient(config);
    }

    public void ca(CA ca) {
        this.ca = ca;
    }

    public String oltpImage() {
        return this.oltpImage;
    }

    public void oltpImage(String oltpImage) {
        this.oltpImage = oltpImage;
    }

    public String olapImage() {
        return this.olapImage;
    }

    public void olapImage(String olapImage) {
        this.olapImage = olapImage;
    }

    public String storageImage() {
        return this.storageImage;
    }

    public void storageImage(String storageImage) {
        this.storageImage = storageImage;
    }

    public Namespace namespace(String ns) {
        NamespaceList nameSpaceList = this.client.namespaces().list();
        List<Namespace> namespaceList = nameSpaceList.getItems();
        for (Namespace namespace : namespaceList) {
            if (namespace.getMetadata().getName().equals(ns)) {
                return namespace;
            }
        }
        return null;
    }

    public List<Namespace> namespaces() {
        NamespaceList nameSpaceList = this.client.namespaces().list();
        return nameSpaceList.getItems();
    }

    public List<String> namespaceNames() {
        List<String> names = new ArrayList<>();
        NamespaceList nameSpaceList = this.client.namespaces().list();
        for (Namespace namespace : nameSpaceList.getItems()) {
            names.add(namespace.getMetadata().getName());
        }
        return names;
    }

    public Namespace createNamespace(String name, Map<String, String> labels) {
        Namespace namespace = new NamespaceBuilder()
                .withNewMetadata()
                .withName(name)
                .addToLabels(labels)
                .endMetadata()
                .build();
        return this.client.namespaces().createOrReplace(namespace);
    }

    public boolean deleteNamespace(String name) {
        return this.client.namespaces().withName(name).delete();
    }

    public Pod createPod(String namespace, String podName,
                         Map<String, String> labels,
                         String containerName, String image) {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                .withName(podName)
                .addToLabels(labels)
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName(containerName)
                .withImage(image)
                .endContainer()
                .endSpec()
                .build();
        return this.client.pods().inNamespace(namespace).createOrReplace(pod);
    }

    public List<Pod> pods(String namespace) {
        return this.client.pods().inNamespace(namespace).list().getItems();
    }

    public Pod pod(String namespace, String podName) {
        return this.client.pods()
                          .inNamespace(namespace)
                          .withName(podName)
                          .get();
    }

    public Set<String> startOltpService(GraphSpace graphSpace,
                                        Service service,
                                        List<String> metaServers,
                                        String cluster) {
        this.createConfigMapForCaIfNeeded(graphSpace, service);
        this.createServcieAccountIfNeeded(graphSpace, service);
        this.createDeployment(graphSpace, service, metaServers, cluster);
        return this.createService(graphSpace, service);
    }

    public void stopOltpService(GraphSpace graphSpace, Service service) {
        String deploymentName = serviceName(graphSpace, service);
        String namespace = namespace(graphSpace, service);
        this.client.apps().deployments().inNamespace(namespace)
                   .withName(deploymentName).delete();
    }

    public void createConfigMapForCaIfNeeded(GraphSpace graphSpace,
                                             Service service) {
        String namespace = namespace(graphSpace, service);
        ConfigMap configMap = this.client.configMaps()
                                         .inNamespace(namespace)
                                         .withName(CA_CONFIG_MAP_NAME)
                                         .get();
        if (configMap != null) {
            return;
        }

        String ca;
        String clientCa;
        String clientKey;
        String config;
        try {
            ca = FileUtils.readFileToString(new File(this.ca.caFile));
            clientCa = FileUtils.readFileToString(
                    new File(this.ca.clientCaFile));
            clientKey = FileUtils.readFileToString(
                    new File(this.ca.clientKeyFile));
            config = FileUtils.readFileToString(new File(this.ca.config()));
        } catch (IOException e) {
            throw new HugeException("Failed to read ca files", e);
        }

        Map<String, String> data = new HashMap<>(4);
        data.put("config", config);
        data.put("ca.pem", ca);
        data.put("kubernetes.pem", clientCa);
        data.put("kubernetes-key8.pem", clientKey);
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(CA_CONFIG_MAP_NAME)
                .withNamespace(namespace)
                .endMetadata()
                .withData(data)
                .build();
        this.client.configMaps()
                   .inNamespace(namespace)
                   .create(cm);
    }

    private void createServcieAccountIfNeeded(GraphSpace graphSpace,
                                              Service service) {
        String namespace = namespace(graphSpace, service);
        ServiceAccount serviceAccount = this.client
                .serviceAccounts()
                .inNamespace(namespace)
                .withName(SERVICE_ACCOUNT_NAME)
                .get();

        if (serviceAccount != null) {
            return;
        }

        // Create service account
        serviceAccount = new ServiceAccountBuilder()
                .withNewMetadata()
                .withName(SERVICE_ACCOUNT_NAME)
                .withNamespace(namespace)
                .endMetadata().build();
        this.client.serviceAccounts()
                   .inNamespace(namespace)
                   .create(serviceAccount);

        // Bind service account
        Subject subject = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName(SERVICE_ACCOUNT_NAME)
                .withNamespace(namespace)
                .build();
        ClusterRoleBinding clusterRoleBinding = new ClusterRoleBindingBuilder()
                .withApiVersion(BINDING_API_VERSION)
                .withNewMetadata()
                .withName(SERVICE_ACCOUNT_NAME)
                .endMetadata()

                .withNewRoleRef()
                .withApiGroup(BINDING_API_GROUP)
                .withKind(CLUSTER_ROLE)
                .withName(CLUSTER_ROLE_NAME)
                .endRoleRef()

                .withSubjects(subject)
                .build();

        this.client.rbac().clusterRoleBindings().create(clusterRoleBinding);
    }

    public Set<String> createService(GraphSpace graphSpace, Service svc) {
        String serviceName = serviceName(graphSpace, svc);
        String namespace = namespace(graphSpace, svc);
        String portName = serviceName + PORT_SUFFIX;
        io.fabric8.kubernetes.api.model.Service service;
        if (NODE_PORT.equals(svc.routeType())) {
            if (svc.port() != 0) {
                service = new ServiceBuilder()
                        .withNewMetadata()
                        .withName(serviceName)
                        .endMetadata()
                        .withNewSpec()
                        .withSelector(Collections.singletonMap(APP, serviceName))
                        .addNewPort()
                        .withName(portName)
                        .withProtocol(TCP)
                        .withPort(HG_PORT)
                        .withTargetPort(new IntOrString(HG_PORT))
                        .withNodePort(svc.port())
                        .endPort()
                        .withType(NODE_PORT)
                        .endSpec()
                        .build();
            } else {
                service = new ServiceBuilder()
                        .withNewMetadata()
                        .withName(serviceName)
                        .endMetadata()
                        .withNewSpec()
                        .withSelector(Collections.singletonMap(APP, serviceName))
                        .addNewPort()
                        .withName(portName)
                        .withProtocol(TCP)
                        .withPort(HG_PORT)
                        .withTargetPort(new IntOrString(HG_PORT))
                        .endPort()
                        .withType(NODE_PORT)
                        .endSpec()
                        .build();
            }
        } else {
            service = new ServiceBuilder()
                    .withNewMetadata()
                    .withName(serviceName)
                    .endMetadata()
                    .withNewSpec()
                    .withSelector(Collections.singletonMap(APP, serviceName))
                    .addNewPort()
                    .withName(portName)
                    .withProtocol(TCP)
                    .withPort(HG_PORT)
                    .withTargetPort(new IntOrString(HG_PORT))
                    .endPort()
                    .withType(svc.routeType())
                    .endSpec()
                    .build();
        }

        this.client.services().inNamespace(namespace).create(service);

        service = this.client.services()
                             .inNamespace(namespace)
                             .withName(serviceName)
                             .get();

        return urlsOfService(service, svc.routeType());
    }

    public Deployment createDeployment(GraphSpace graphSpace, Service service,
                                       List<String> metaServers,
                                       String cluster) {
        Deployment deployment = this.constructDeployment(graphSpace, service,
                                                         metaServers, cluster);
        String namespace = namespace(graphSpace, service);
        deployment = this.client.apps().deployments().inNamespace(namespace)
                                .createOrReplace(deployment);

        ListOptions options = new ListOptions();
        options.setLabelSelector(APP + "=" + serviceName(graphSpace, service));
        List<Pod> hugegraphservers = new ArrayList<>();
        int count = 0;
        while (hugegraphservers.isEmpty() && count++ < 10) {
            hugegraphservers = this.client.pods()
                                          .inNamespace(namespace)
                                          .list(options)
                                          .getItems();
            sleepAWhile(1);
        }
        if (hugegraphservers.isEmpty()) {
            throw new HugeException("Failed to start oltp server pod");
        }
        return deployment;
    }

    private static Set<String> urlsOfService(
            io.fabric8.kubernetes.api.model.Service service, String routeType) {
        Set<String> urls = new HashSet<>();
        String clusterIP = service.getSpec().getClusterIP();
        for (ServicePort port : service.getSpec().getPorts()) {
            int actualPort = routeType.equals(NODE_PORT) ?
                             port.getNodePort() : port.getPort();
            urls.add(clusterIP + COLON + actualPort);
        }
        return urls;
    }

    private Deployment constructDeployment(GraphSpace graphSpace,
                                           Service service,
                                           List<String> metaServers,
                                           String cluster) {
        String deploymentName = deploymentName(graphSpace, service);
        String containerName = String.join(DELIMETER, deploymentName,
                                           CONTAINER);
        Quantity cpu = Quantity.parse((service.cpuLimit() * 1000) + CPU_UNIT);
        Quantity memory = Quantity.parse(service.memoryLimit() + MEMORY_UNIT);
        ResourceRequirements rr = new ResourceRequirementsBuilder()
                .addToLimits(CPU, cpu)
                .addToLimits(MEMORY, memory)
                .build();

        HTTPGetAction readyProbeAction = new HTTPGetActionBuilder()
                .withPath(HEALTH_CHECK_API)
                .withPort(new IntOrString(HG_PORT))
                .build();

        ConfigMapVolumeSource cmvs = new ConfigMapVolumeSourceBuilder()
                .withName(CA_CONFIG_MAP_NAME)
                .build();

        String metaServersString = metaServers(metaServers);

        EnvVarSource nodeIP = new EnvVarSourceBuilder()
                .withNewFieldRef()
                .withFieldPath(SPEC_NODE_NAME)
                .endFieldRef()
                .build();
        EnvVarSource podIP = new EnvVarSourceBuilder()
                .withNewFieldRef()
                .withFieldPath(STATUS_POD_IP)
                .endFieldRef()
                .build();

        return new DeploymentBuilder()

                .withNewMetadata()
                .withName(deploymentName)
                .addToLabels(APP, deploymentName)
                .endMetadata()

                .withNewSpec()
                .withReplicas(service.count())
                .withNewTemplate()

                .withNewMetadata()
                .addToLabels(APP, deploymentName)
                .endMetadata()

                .withNewSpec()
                .withServiceAccountName(SERVICE_ACCOUNT_NAME)
                .withAutomountServiceAccountToken(true)

                .addNewContainer()
                .withName(containerName)
                .withImage(this.image(service))
                .withResources(rr)

                .withNewReadinessProbe()
                .withHttpGet(readyProbeAction)
                .withInitialDelaySeconds(30)
                .withPeriodSeconds(5)
                .endReadinessProbe()

                .addNewPort()
                .withContainerPort(HG_PORT)
                .endPort()

                .addNewVolumeMount()
                .withName(CA_CONFIG_MAP_NAME)
                .withMountPath(CA_CONFIG_MAP_NAME)
                .endVolumeMount()

                .addNewEnv()
                .withName(GRAPH_SPACE)
                .withValue(graphSpace.name())
                .endEnv()
                .addNewEnv()
                .withName(SERVICE_ID)
                .withValue(service.name())
                .endEnv()
                .addNewEnv()
                .withName(META_SERVERS)
                .withValue(metaServersString)
                .endEnv()
                .addNewEnv()
                .withName(CLUSTER)
                .withValue(cluster)
                .endEnv()
                .addNewEnv()
                .withName(MY_NODE_NAME)
                .withValueFrom(nodeIP)
                .endEnv()
                .addNewEnv()
                .withName(MY_POD_IP)
                .withValueFrom(podIP)
                .endEnv()
                .addNewEnv()
                .withName(APP_NAME)
                .withValue(deploymentName)
                .endEnv()

                .endContainer()

                .addNewVolume()
                .withName(CA_CONFIG_MAP_NAME)
                .withConfigMap(cmvs)
                .endVolume()

                .endSpec()
                .endTemplate()
                .withNewSelector()
                .addToMatchLabels(APP, deploymentName)
                .endSelector()
                .endSpec()
                .build();
    }

    private static String metaServers(List<String> metaServers) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < metaServers.size(); i++) {
            builder.append(metaServers.get(i));
            if (i != metaServers.size() - 1) {
                builder.append(COMMA);
            }
        }
        return builder.toString();
    }

    private static String namespace(GraphSpace graphSpace, Service service) {
        String namespace;
        switch (service.type()) {
            case OLTP:
                namespace = graphSpace.oltpNamespace;
                break;
            case OLAP:
            case STORAGE:
            default:
                throw new AssertionError(String.format(
                          "Invalid service type '%s'", service.type()));
        }
        return namespace;
    }

    private String image(Service service) {
        switch (service.type()) {
            case OLTP:
                return this.oltpImage;
            case OLAP:
                return this.olapImage;
            case STORAGE:
                return this.storageImage;
            default:
                throw new AssertionError(String.format(
                          "Invalid service type '%s'", service.type()));
        }
    }

    private static String serviceName(String graphSpace,
                                      Service service) {
        return String.join(DELIMETER, graphSpace,
                           service.type().name().toLowerCase(), service.name());
    }

    private static String deploymentName(GraphSpace graphSpace,
                                         Service service) {
        return String.join(DELIMETER, graphSpace.name(),
                           service.type().name().toLowerCase(), service.name());
    }

    private static String serviceName(GraphSpace graphSpace,
                                      Service service) {
        return String.join(DELIMETER, graphSpace.name(),
                           service.type().name().toLowerCase(), service.name());
    }

    private static void sleepAWhile(int second) {
        try {
            Thread.sleep(second * 1000L);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    public int podsRunning(GraphSpace graphSpace, Service service) {
        String deploymentName = deploymentName(graphSpace, service);
        String namespace = namespace(graphSpace, service);
        Deployment deployment;
        deployment = this.client.apps().deployments()
                         .inNamespace(namespace)
                         .withName(deploymentName)
                         .get();
        return deployment.getStatus().getReadyReplicas();
    }

    public static class CA {

        private static final String CONFIG_PATH_SUFFIX = "/.kube/config";
        private static final String USER_HOME = "user.home";

        private String caFile;
        private String clientCaFile;
        private String clientKeyFile;

        public CA(String caFile, String clientCaFile, String clientKeyFile) {
            E.checkArgument(caFile != null && !caFile.isEmpty(),
                            "The ca file can't be null or empty");
            E.checkArgument(clientCaFile != null && !clientCaFile.isEmpty(),
                            "The client ca file can't be null or empty");
            E.checkArgument(clientKeyFile != null && !clientKeyFile.isEmpty(),
                            "The client key file can't be null or empty");
            this.caFile = caFile;
            this.clientCaFile = clientCaFile;
            this.clientKeyFile = clientKeyFile;
        }

        public String caFile() {
            return this.caFile;
        }

        public String clientCaFile() {
            return this.clientCaFile;
        }

        public String clientKeyFile() {
            return this.clientKeyFile;
        }

        public String config() {
            return System.getProperty(USER_HOME) + CONFIG_PATH_SUFFIX;
        }
    }
}
