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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.space.Service;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.base.Strings;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Pod;

public class K8sManager {

    private static final HugeGraphLogger LOGGER =
        Log.getLogger(K8sManager.class);

    private K8sDriver k8sDriver;

    private static final K8sManager INSTANCE = new K8sManager();

    private String operatorTemplate;

    public static K8sManager instance() {
        return INSTANCE;
    }

    private K8sManager() {
    }

    public void connect(String url, String caFile, String clientCaFile,
                        String clientKeyFile, String oltpImage,
                        String olapImage, String storageImage,
                        K8sDriver.CA ca) {
        E.checkArgument(url != null && !url.isEmpty(),
                        "The url of k8s can't be null or empty");
        this.k8sDriver = caFile == null || caFile.isEmpty() ?
                         new K8sDriver(url) :
                         new K8sDriver(url, caFile, clientCaFile,
                                       clientKeyFile);
        this.k8sDriver.ca(ca);
        this.k8sDriver.oltpImage(oltpImage);
        this.k8sDriver.olapImage(olapImage);
        this.k8sDriver.storageImage(storageImage);
    }

    private void loadOperatorTemplate() {
        if (!Strings.isNullOrEmpty(this.operatorTemplate)) {
            return;
        }
        try {
            File file = new File(CoreOptions.K8S_OPERATOR_TEMPLATE.defaultValue());
            FileReader reader = new FileReader(file);
            int length = (int)file.length();
            char buffer[] = new char[length];
            reader.read(buffer, 0, length);
            this.operatorTemplate = new String(buffer);
            reader.close();

        } catch (IOException exc) {

        } finally {

        }
    }

    public Namespace namespace(String ns) {
        return this.k8sDriver.namespace(ns);
    }

    public Namespace createNamespace(String namespace, Map<String, String> labelMap) {
        return this.k8sDriver.createNamespace(namespace, labelMap);
    }

    @SuppressWarnings("unchecked")
    public Set<String> startOltpService(GraphSpace graphSpace,
                                        Service service,
                                        List<String> metaServers,
                                        String cluster) {
        
        if (null == k8sDriver) {
            LOGGER.logCriticalError(new HugeException("k8sDriver is not initialized!"), "startOltpService");
            return Collections.EMPTY_SET;
        }
        return this.k8sDriver.startOltpService(graphSpace, service,
                                               metaServers, cluster);
    }

    public Set<String> startService(GraphSpace graphSpace, Service service,
                                    List<String> metaServers, String cluster) {
        switch (service.type()) {
            case OLTP:
                return this.startOltpService(graphSpace, service, metaServers,
                                             cluster);
            case OLAP:
            case STORAGE:
            default:
                throw new AssertionError(String.format(
                          "Invalid service type '%s'", service.type()));
        }
    }

    public void stopService(GraphSpace graphSpace, Service service) {
        if (null == k8sDriver) {
            LOGGER.logCriticalError(new HugeException("k8sDriver is not initialized!"), "stopService");
            return;
        }
        switch (service.type()) {
            case OLTP:
                this.k8sDriver.stopOltpService(graphSpace, service);
                break;
            default:
                LOGGER.logCustomDebug("Cannot stop service other than OLTP", "K8sManager");
                break;
        }
    }

    public int podsRunning(GraphSpace graphSpace, Service service) {
        if (null == k8sDriver) {
            throw new HugeException("k8sDriver is not initialized!");
        }
        return this.k8sDriver.podsRunning(graphSpace, service);
    }

    /**
     * 
     * @param namespace
     * @param podName 
     * @param labels
     * @param containerName
     * @param image
     * @return
     */
    public void createOperatorPod(String namespace) {
        this.loadOperator(namespace);
               
    }

    // 把所有字符串hugegraph-computer-operator-system都替换成新的namespace就行了
    public void loadOperator(String namespace) throws HugeException {
        this.loadOperatorTemplate();
        if (Strings.isNullOrEmpty(this.operatorTemplate)) {
            throw new HugeException("cannot generate yaml config for operator: template load failed");
        }

        String content = this.operatorTemplate.replace("", namespace);
        

    }
}
