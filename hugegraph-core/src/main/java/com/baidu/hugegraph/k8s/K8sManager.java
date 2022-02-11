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

import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.space.Service;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.Namespace;

public class K8sManager {

    private static final HugeGraphLogger LOGGER =
        Log.getLogger(K8sManager.class);

    private K8sDriver k8sDriver;

    private static final K8sManager INSTANCE = new K8sManager();

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

    public Namespace namespace(String ns) {
        return this.k8sDriver.namespace(ns);
    }

    public Set<String> startOltpService(GraphSpace graphSpace,
                                        Service service,
                                        List<String> metaServers,
                                        String cluster) {
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
        return this.k8sDriver.podsRunning(graphSpace, service);
    }
}
