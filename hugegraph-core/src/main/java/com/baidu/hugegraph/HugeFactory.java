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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;

public class HugeFactory {

    private static final String NAME_REGEX = "^[A-Za-z][A-Za-z0-9_]{0,47}$";

    private static final Map<String, HugeGraph> graphs = new HashMap<>();

    public static synchronized HugeGraph open(Configuration config) {
        HugeConfig conf = new HugeConfig(config);
        String name = conf.get(CoreOptions.STORE);
        checkGraphName(name, "graph config(like hugegraph.properties)");
        name = name.toLowerCase();
        HugeGraph graph = graphs.get(name);
        if (graph == null || graph.closed()) {
            graph = new HugeGraph(conf);
            graphs.put(name, graph);
        } else {
            String backend = conf.get(CoreOptions.BACKEND);
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

    private static PropertiesConfiguration getLocalConfig(String path) {
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

    private static PropertiesConfiguration getRemoteConfig(URL url) {
        try {
            return new PropertiesConfiguration(url);
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load remote config file: %s",
                                    e, url);
        }
    }
}
