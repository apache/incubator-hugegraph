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
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.config.HugeConfig;

public final class ConfigUtil {

    private static final Logger LOG = Log.logger(ConfigUtil.class);

    public static final String SUFFIX = ".properties";

    public static Map<String, String> scanGraphsDir(String graphsDirPath) {
        LOG.info("Scaning graphs configuration directory {}", graphsDirPath);
        File graphsDir = new File(graphsDirPath);
        E.checkArgument(graphsDir.exists() && graphsDir.isDirectory(),
                        "Please ensure the graphs configuration directory " +
                                "exist and indeed a directory");
        File[] confFiles = graphsDir.listFiles((dir, name) -> {
            return name.endsWith(SUFFIX);
        });
        E.checkNotNull(confFiles, "graph configuration files");
        E.checkArgument(confFiles.length != 0,
                        "graph configuration files must be exist at least one");
        Map<String, String> graphConfs = InsertionOrderUtil.newMap();
        for (File confFile : confFiles) {
            // NOTE: file name as graph name
            String graphName = StringUtils.substringBefore(confFile.getName(),
                                                           ConfigUtil.SUFFIX);
            graphConfs.put(graphName, confFile.getPath());
        }
        return graphConfs;
    }

    public static void writeFile(String dir, String graphName,
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
}
