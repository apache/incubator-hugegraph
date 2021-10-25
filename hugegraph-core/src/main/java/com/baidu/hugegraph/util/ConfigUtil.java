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
import java.io.Writer;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import javax.ws.rs.NotSupportedException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.config.HugeConfig;

public final class ConfigUtil {

    private static final Logger LOG = Log.logger(ConfigUtil.class);

    private static final String NODE_GRAPHS = "graphs";
    private static final String SUFFIX = ".properties";

    public static void checkGremlinConfig(String conf) {
        YamlConfiguration yamlConfig = new YamlConfiguration();
        try {
            yamlConfig.load(conf);
        } catch (ConfigurationException e) {
            throw new HugeException(
                      "Failed to load yaml config file %s", e, conf);
        }
        List<ConfigurationNode> nodes = yamlConfig.getRootNode()
                                                  .getChildren(NODE_GRAPHS);
        E.checkArgument(nodes == null || nodes.size() == 1,
                        "Not allowed to specify multiple '%s' nodes in " +
                        "config file '%s'", NODE_GRAPHS, conf);
    }

    public static Map<String, String> scanGraphsDir(String graphsDirPath) {
        LOG.info("Scanning graphs configuration directory {}", graphsDirPath);
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

    public static void writeToFile(String dir, String graphName,
                                   HugeConfig config) {
        E.checkArgument(FileUtils.getFile(dir).exists(),
                        "The graphs conf directory must exist");
        String fileName = Paths.get(dir, graphName + SUFFIX).toString();
        try (OutputStream os = new FileOutputStream(fileName)) {
            config.save(os, Charsets.UTF_8.name());
            config.setFileName(fileName);
            LOG.info("Write HugeConfig to file {}", fileName);
        } catch (IOException | ConfigurationException e) {
            throw new HugeException("Failed to write HugeConfig to file {}",
                                    e, fileName);
        }
    }

    public static String writeConfigToString(HugeConfig config) {
        String content;
        try {
            if (config.getFileName() == null) {
                Writer writer = new StringBuilderWriter();
                config.save(writer);
                content = writer.toString();
            } else {
                File file = config.getFile();
                if (file == null) {
                    throw new NotSupportedException(
                              "Can't access the api in a node which started " +
                              "with non local file config.");
                }
                content = FileUtils.readFileToString(file);
            }
        } catch (ConfigurationException | IOException e) {
            throw new HugeException("Failed to read config of graph", e);
        }
        return content;
    }

    public static void deleteFile(File file) {
        try {
            FileUtils.forceDelete(file);
        } catch (IOException e) {
            throw new HugeException("Failed to delete HugeConfig file {}",
                                    e, file);
        }
    }
}
