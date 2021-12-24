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

package com.baidu.hugegraph;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;

import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.traversal.algorithm.OltpTraverser;
import com.baidu.hugegraph.type.define.SerialEnum;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class HugeFactory {

    private static final Logger LOG = Log.logger(HugeGraph.class);

    static {
        SerialEnum.registerInternalEnums();
        HugeGraph.registerTraversalStrategies(StandardHugeGraph.class);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("HugeGraph is shutting down");
            HugeFactory.shutdown(30L);
        }, "hugegraph-shutdown"));
    }

    private static final String NAME_REGEX = "^[A-Za-z][A-Za-z0-9_]{0,47}$";

    private static final Map<String, HugeGraph> graphs = new HashMap<>();

    public static synchronized HugeGraph open(Configuration config) {
        HugeConfig conf = config instanceof HugeConfig ?
                          (HugeConfig) config : new HugeConfig(config);
        return open(conf);
    }

    public static synchronized HugeGraph open(HugeConfig config) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // Not allowed to read file via Gremlin when SecurityManager enabled
            String configFile = config.getFileName();
            if (configFile == null) {
                configFile = config.toString();
            }
            sm.checkRead(configFile);
        }

        String name = config.get(CoreOptions.STORE);
        checkGraphName(name, "graph config(like hugegraph.properties)");
        name = name.toLowerCase();
        HugeGraph graph = graphs.get(name);
        if (graph == null || graph.closed()) {
            graph = new StandardHugeGraph(config);
            graphs.put(name, graph);
        } else {
            String backend = config.get(CoreOptions.BACKEND);
            E.checkState(backend.equalsIgnoreCase(graph.backend()),
                         "Graph name '%s' has been used by backend '%s'",
                         name, graph.backend());
        }
        return graph;
    }

    public static HugeGraph open(String path) {
        return open(getLocalConfig(path));
    }

    public static HugeGraph open(URL url) {
        return open(getRemoteConfig(url));
    }

    public static void checkGraphName(String name, String configFile) {
        E.checkArgument(name.matches(NAME_REGEX),
                        "Invalid graph name '%s' in %s, " +
                        "valid graph name is up to 48 alpha-numeric " +
                        "characters and underscores and only letters are " +
                        "supported as first letter. " +
                        "Note: letter is case insensitive", name, configFile);
    }

    public static PropertiesConfiguration getLocalConfig(String path) {
        File file = new File(path);
        E.checkArgument(file.exists() && file.isFile() && file.canRead(),
                        "Please specify a proper config file rather than: %s",
                        file.toString());
        try {
            return new PropertiesConfiguration(file);
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load config file: %s", e, path);
        }
    }

    public static PropertiesConfiguration getRemoteConfig(URL url) {
        try {
            return new PropertiesConfiguration(url);
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load remote config file: %s",
                                    e, url);
        }
    }

    /**
     * Stop all the daemon threads
     * @param timeout seconds
     */
    public static void shutdown(long timeout) {
        try {
            if (!EventHub.destroy(timeout)) {
                throw new TimeoutException(timeout + "s");
            }
            TaskManager.instance().shutdown(timeout);
            OltpTraverser.destroy();
        } catch (Throwable e) {
            LOG.error("Error while shutdown", e);
            throw new HugeException("Failed to shutdown", e);
        }
    }

    public static void remove(HugeGraph graph) {
        HugeConfig config = (HugeConfig) graph.configuration();
        String name = config.get(CoreOptions.STORE);
        graphs.remove(name);
    }
}
