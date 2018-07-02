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

package com.baidu.hugegraph.cmd;

import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.store.BackendStoreInfo;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.dist.RegisterUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class InitStore {

    private static final Logger LOG = Log.logger(InitStore.class);

    private static final String GRAPHS = ServerOptions.GRAPHS.name();
    // 6~8 retries may be needed under high load for Cassandra backend
    private static final int RETRIES = 10;
    // Less than 5000 may cause mismatch exception with Cassandra backend
    private static final long RETRY_INTERVAL = 5000;

    private static MultiValueMap exceptions = new MultiValueMap();

    static {
        exceptions.put("OperationTimedOutException",
                       "Timed out waiting for server response");
        exceptions.put("NoHostAvailableException",
                       "All host(s) tried for query failed");
        exceptions.put("InvalidQueryException", "does not exist");
        exceptions.put("InvalidQueryException", "unconfigured table");
    }

    public static void main(String[] args)
                       throws ConfigurationException, InterruptedException {
        E.checkArgument(args.length == 1,
                        "Init store only accept one config file.");
        E.checkArgument(args[0].endsWith(".yaml"),
                        "Init store only accept yaml config file.");

        String confFile = args[0];
        RegisterUtil.registerBackends();

        YamlConfiguration config = new YamlConfiguration();
        config.load(confFile);

        List<ConfigurationNode> nodes = config.getRootNode()
                                              .getChildren(GRAPHS);
        E.checkArgument(nodes.size() == 1,
                        "Must contain one '%s' in config file '%s'",
                        GRAPHS, confFile);

        List<ConfigurationNode> graphNames = nodes.get(0).getChildren();

        E.checkArgument(!graphNames.isEmpty(),
                        "Must contain at least one graph");

        for (ConfigurationNode graphName : graphNames) {
            String configPath = graphName.getValue().toString();
            initGraph(configPath);
        }

        HugeGraph.shutdown(30L);
    }

    private static void initGraph(String config) throws InterruptedException {
        LOG.info("Init graph with config file: {}", config);
        HugeGraph graph = HugeFactory.open(config);

        BackendStoreInfo backendStoreInfo = new BackendStoreInfo(graph);
        try {
            if (backendStoreInfo.exist()) {
                LOG.info("Skip init-store due to the backend store of '{}' " +
                         "had been initialized", graph.name());
                backendStoreInfo.checkVersion();
            } else {
                initBackend(graph);
            }
        } finally {
            graph.close();
        }
    }

    private static void initBackend(final HugeGraph graph)
                                    throws InterruptedException {
        int retries = RETRIES;
        retry: do {
            try {
                graph.initBackend();
            } catch (Exception e) {
                String clz = e.getClass().getSimpleName();
                String message = e.getMessage();
                if (exceptions.containsKey(clz) && retries > 0) {
                    @SuppressWarnings("unchecked")
                    Collection<String> keywords = exceptions.getCollection(clz);
                    for (String keyword : keywords) {
                        if (message.contains(keyword)) {
                            LOG.info("Init failed with exception '{} : {}', " +
                                     "retry  {}...",
                                     clz, message, RETRIES - retries + 1);

                            Thread.sleep(RETRY_INTERVAL);
                            continue retry;
                        }
                    }
                }
                throw e;
            }
            break;
        } while(retries-- > 0);
    }
}
