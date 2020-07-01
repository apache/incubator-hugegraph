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

package com.baidu.hugegraph.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.config.HugeConfig;
import com.google.common.collect.ImmutableList;

public final class ConfigUtil {

    private static final Logger LOG = Log.logger(ConfigUtil.class);

    private static final String NODE_GRAPHS = "graphs";
    private static final String SUFFIX = ".properties";

    public static Map<String, String> parseGremlinGraphs(String conf) {
        YamlConfiguration yamlConfig = new YamlConfiguration();
        try {
            yamlConfig.load(conf);
        } catch (ConfigurationException e) {
            throw new HugeException("Failed to load yaml config file ", conf);
        }
        List<ConfigurationNode> nodes = yamlConfig.getRootNode()
                                                  .getChildren(NODE_GRAPHS);
        E.checkArgument(nodes.size() <= 1,
                        "There is at most one '%s' node in config file '%s'",
                        NODE_GRAPHS, conf);
        nodes = !nodes.isEmpty() ? nodes.get(0).getChildren() :
                ImmutableList.of();

        Map<String, String> gremlinGraphs = InsertionOrderUtil.newMap();
        for (ConfigurationNode node : nodes) {
            @SuppressWarnings("unchecked")
            String name = ((Map.Entry<String, Object>)
                          node.getReference()).getKey();
            HugeFactory.checkGraphName(name, conf);
            gremlinGraphs.put(name, node.getValue().toString());
        }
        return gremlinGraphs;
    }

    public static Map<String, String> scanGraphsDir(String graphsDirPath) {
        LOG.info("Scaning graphs configuration directory {}", graphsDirPath);
        File graphsDir = new File(graphsDirPath);
        E.checkArgument(graphsDir.exists() && graphsDir.isDirectory(),
                        "Please ensure the graphs config directory '%s' " +
                        "exist and indeed a directory", graphsDir);
        File[] confFiles = graphsDir.listFiles((dir, name) -> {
            return name.endsWith(SUFFIX);
        });
        E.checkNotNull(confFiles, "graph configuration files");
        Map<String, String> graphConfs = InsertionOrderUtil.newMap();
        for (File confFile : confFiles) {
            // NOTE: file name as graph name
            String name = StringUtils.substringBefore(confFile.getName(),
                                                      ConfigUtil.SUFFIX);
            HugeFactory.checkGraphName(name, confFile.getPath());
            graphConfs.put(name, confFile.getPath());
        }
        return graphConfs;
    }

    public static Map<String, String> mergeGraphConfs(String gremlinConf,
                                                      String graphsDir) {
        Map<String, String> gremlinGraphs = parseGremlinGraphs(gremlinConf);
        Map<String, String> graphs = scanGraphsDir(graphsDir);
        return mergeGraphConfs(gremlinGraphs, graphs);
    }

    public static Map<String, String> mergeGraphConfs(
                                      Map<String, String> gremlinGraphs,
                                      Map<String, String> graphs) {
        E.checkArgument(!gremlinGraphs.isEmpty() || !graphs.isEmpty(),
                        "At least one graph must be configured");

        Map<String, String> mergedGraphs = InsertionOrderUtil.newMap();
        if (!gremlinGraphs.isEmpty() && !graphs.isEmpty()) {
            for (Map.Entry<String, String> entry : gremlinGraphs.entrySet()) {
                String graphName = entry.getKey();
                String gremlinConfigPath = entry.getValue();
                assert gremlinConfigPath != null;
                /*
                 * The rule is
                 * 1. exist duplicate graph name
                 *   1.1 same config file, count only one, OK
                 *   1.2 not same config file, CONFLICT
                 * 2. unexist duplicate graph name
                 *   2.1 same config file, CONFLICT
                 *   2.2 not same config file, OK
                 */
                String configPath = graphs.get(entry.getKey());
                if (configPath == null) {
                    // All values in graphs are not same
                    if (graphs.values().contains(gremlinConfigPath)) { // 2.1
                        throw new HugeException("There exist some graphs use " +
                                                "same config file %s",
                                                gremlinConfigPath);
                    }
                } else if (!gremlinConfigPath.equals(configPath)) { // 1.2
                    throw new HugeException("Duplicate graph name '%s' with " +
                                            "different config files %s and %s",
                                            entry.getKey(), gremlinConfigPath,
                                            configPath);
                }
                mergedGraphs.put(graphName, gremlinConfigPath);
            }
        }
        mergedGraphs.putAll(graphs);
        E.checkArgument(!mergedGraphs.isEmpty(),
                        "There should exist at least one graph conf");
        return mergedGraphs;
    }

    public static void writeToFile(String dir, String graphName,
                                   HugeConfig config) {
        E.checkArgument(FileUtils.getFile(dir).exists(),
                        "The graphs conf directory must exist");
        String fileName = Paths.get(dir, graphName + SUFFIX).toString();
        try (OutputStream os = new FileOutputStream(fileName)) {
            config.save(os, Charsets.UTF_8.name());
            LOG.info("Write HugeConfig to file {}", fileName);
        } catch (IOException | ConfigurationException e) {
            throw new HugeException("Failed to write HugeConfig to file {}",
                                    fileName);
        }
    }

    public static void copyFiles(String dir, Map<String, String> graphs) {
        for (Map.Entry<String, String> entry : graphs.entrySet()) {
            File srcConfigFile = new File(entry.getValue());
            String destFileName = Paths.get(dir, entry.getKey()).toString();
            File destConfigFile = new File(destFileName);
            try {
                FileUtils.copyFile(srcConfigFile, destConfigFile);
            } catch (IOException e) {
                throw new HugeException("Failed to copy config file %s to %s",
                                        srcConfigFile, destConfigFile);
            }
        }
    }
}
