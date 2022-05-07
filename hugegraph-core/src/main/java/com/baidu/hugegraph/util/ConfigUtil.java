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
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.NodeHandler;
import org.apache.commons.configuration2.tree.NodeModel;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.config.HugeConfig;

public final class ConfigUtil {

    private static final Logger LOG = Log.logger(ConfigUtil.class);

    private static final String NODE_GRAPHS = "graphs";
    private static final String CONF_SUFFIX = ".properties";
    private static final String CHARSET = "UTF-8";

    public static void checkGremlinConfig(String conf) {
        Parameters params = new Parameters();
        try {

            FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
            new FileBasedConfigurationBuilder(YAMLConfiguration.class)
                .configure(params.fileBased().setFileName(conf));
            YAMLConfiguration config = (YAMLConfiguration) builder
                                                           .getConfiguration();

            List<HierarchicalConfiguration<ImmutableNode>> nodes =
                                           config.childConfigurationsAt(
                                           NODE_GRAPHS);
            if (nodes == null || nodes.isEmpty()) {
                return;
            }
            E.checkArgument(nodes.size() == 1,
                            "Not allowed to specify multiple '%s' " +
                            "nodes in config file '%s'", NODE_GRAPHS, conf);

            ImmutableNode root = null;
            NodeHandler<ImmutableNode> nodeHandler = null;
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                NodeModel<ImmutableNode> nodeModel = node.getNodeModel();
                E.checkArgument(nodeModel != null &&
                  (nodeHandler = nodeModel.getNodeHandler()) != null &&
                  (root = nodeHandler.getRootNode()) != null,
                  "Node '%s' must contain root", node);
            }
        } catch (ConfigurationException e) {
            throw new HugeException("Failed to load yaml config file '%s'",
                                    conf);
        }
    }

    public static Map<String, String> scanGraphsDir(String graphsDirPath) {
        LOG.info("Scanning option 'graphs' directory '{}'", graphsDirPath);
        File graphsDir = new File(graphsDirPath);
        E.checkArgument(graphsDir.exists() && graphsDir.isDirectory(),
                        "Please ensure the path '%s' of option 'graphs' " +
                        "exist and it's a directory", graphsDir);
        File[] confFiles = graphsDir.listFiles((dir, name) -> {
            return name.endsWith(CONF_SUFFIX);
        });
        E.checkNotNull(confFiles, "graph configuration files");
        Map<String, String> graphConfs = InsertionOrderUtil.newMap();
        for (File confFile : confFiles) {
            // NOTE: use file name as graph name
            String name = StringUtils.substringBefore(confFile.getName(),
                                                      ConfigUtil.CONF_SUFFIX);
            HugeFactory.checkGraphName(name, confFile.getPath());
            graphConfs.put(name, confFile.getPath());
        }
        return graphConfs;
    }

    public static String writeToFile(String dir, String graphName,
                                     HugeConfig config) {
        E.checkArgument(FileUtils.getFile(dir).exists(),
                        "The directory '%s' must exist", dir);
        String fileName = Paths.get(dir, graphName + CONF_SUFFIX).toString();
        try {
            config.save(new File(fileName));
            LOG.info("Write HugeConfig to file: '{}'", fileName);
        } catch (ConfigurationException e) {
            throw new HugeException("Failed to write HugeConfig to file '%s'",
                                    e, fileName);
        }

        return fileName;
    }

    public static void deleteFile(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        try {
            FileUtils.forceDelete(file);
        } catch (IOException e) {
            throw new HugeException("Failed to delete HugeConfig file '%s'",
                                    e, file);
        }
    }

    public static PropertiesConfiguration buildConfig(String configText) {
        E.checkArgument(StringUtils.isNotEmpty(configText),
                        "The config text can't be null or empty");
        PropertiesConfiguration propConfig = new PropertiesConfiguration();
        try {
            Reader in = new StringReader(configText);
            propConfig.read(in);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read config options", e);
        }
        return propConfig;
    }
}
