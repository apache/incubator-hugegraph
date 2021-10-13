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

package com.baidu.hugegraph.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;

import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.collection.CollectionFactory;

public class MetaManager {

    public static final String META_PATH_DELIMETER = "/";
    public static final String META_PATH_JOIN = "-";

    public static final String META_PATH_HUGEGRAPH = "HUGEGRAPH";
    public static final String META_PATH_NAMESPACE = "NAMESPACE";
    public static final String META_PATH_GRAPH_CONF = "GRAPH_CONF";
    public static final String META_PATH_CONF = "CONF";
    public static final String META_PATH_GRAPH = "GRAPH";
    public static final String META_PATH_AUTH = "AUTH";
    public static final String META_PATH_USER = "USER";
    public static final String META_PATH_EVENT = "EVENT";
    public static final String META_PATH_ADD = "ADD";
    public static final String META_PATH_REMOVE = "REMOVE";

    public static final String DEFAULT_NAMESPACE = "default_ns";

    private MetaDriver metaDriver;
    private String cluster;

    private static final MetaManager INSTANCE = new MetaManager();

    public static MetaManager instance() {
        return INSTANCE;
    }

    private MetaManager() {
    }

    public void connect(String cluster, MetaDriverType type, Object... args) {
        E.checkArgument(cluster != null && !cluster.isEmpty(),
                        "The cluster can't be null or empty");
        this.cluster = cluster;

        switch (type) {
            case ETCD:
                this.metaDriver = new EtcdMetaDriver(args);
                break;
            case PD:
                // TODO: uncomment after implement PdMetaDriver
                // this.metaDriver = new PdMetaDriver(args);
                break;
            default:
                throw new AssertionError(String.format(
                          "Invalid meta driver type: %s", type));
        }
    }

    public <T> void listenGraphAdd(Consumer<T> consumer) {
        this.listen(this.graphAddKey(), consumer);
    }

    public <T> void listenGraphRemove(Consumer<T> consumer) {
        this.listen(this.graphRemoveKey(), consumer);
    }

    private <T> void listen(String key, Consumer<T> consumer) {
        this.metaDriver.listen(key, consumer);
    }

    public Map<String, String> graphConfigs() {
        Map<String, String> configs =
                            CollectionFactory.newMap(CollectionType.EC);
        Map<String, String> keyValues = this.metaDriver
                                            .scanWithPrefix(this.confPrefix());
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            String[] parts = key.split(META_PATH_DELIMETER);
            configs.put(parts[parts.length - 1], entry.getValue());
        }
        return configs;
    }

    public <T> List<Pair<String, String>> extractGraphsFromResponse(T response) {
        List<Pair<String, String>> graphs = new ArrayList<>();
        for (String value : this.metaDriver.extractValuesFromResponse(response)) {
            String[] values = value.split(META_PATH_JOIN);
            E.checkArgument(values.length == 2,
                            "Graph name must be '{namespace}-{graph}', " +
                            "but got '%s'", value);
            String namespace = values[0];
            String graphName = values[1];
            graphs.add(Pair.of(namespace, graphName));
        }
        return graphs;
    }

    public String getGraphConfig(String graph) {
        return this.metaDriver.get(this.graphConfKey(graph));
    }

    public void addGraphConfig(String graph, String config) {
        this.metaDriver.put(this.graphConfKey(graph), config);
    }

    public void removeGraphConfig(String graph) {
        this.metaDriver.delete(this.graphConfKey(graph));
    }

    public void addGraph(String graph) {
        this.metaDriver.put(this.graphAddKey(), this.nsGraphName(graph));
    }

    public void removeGraph(String graph) {
        this.metaDriver.put(this.graphRemoveKey(), this.nsGraphName(graph));
    }

    private String graphAddKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/ADD
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_ADD);
    }

    private String graphRemoveKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/REMOVE
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_REMOVE);
    }

    private String confPrefix() {
        return this.graphConfKey(Strings.EMPTY);
    }

    // TODO: remove after support namespace
    private String graphConfKey(String graph) {
        return this.graphConfKey(DEFAULT_NAMESPACE, graph);
    }

    private String graphConfKey(String namespace, String graph) {
        // HUGEGRAPH/{cluster}/NAMESPACE/{namespace}/GRAPH_CONF
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_NAMESPACE,
                           namespace, META_PATH_GRAPH_CONF, graph);
    }

    // TODO: remove after support namespace
    private String nsGraphName(String name) {
        return this.nsGraphName(DEFAULT_NAMESPACE, name);
    }

    private String nsGraphName(String namespace, String name) {
        return String.join(META_PATH_JOIN, namespace, name);
    }

    public enum MetaDriverType {
        ETCD,
        PD
    }
}
