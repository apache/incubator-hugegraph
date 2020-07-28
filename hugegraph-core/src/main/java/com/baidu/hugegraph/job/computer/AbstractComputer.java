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

package com.baidu.hugegraph.job.computer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.job.ComputerJob;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ParameterUtil;

public abstract class AbstractComputer implements Computer {

    private static final String COMMAND_PREFIX =
            "cd $COMPUTER_HOME;" +
            "hadoop jar hugegraph-computer.jar com.baidu.hugegraph.Computer " +
            "-D libjars=./hugegraph-computer-core.jar";

    private static final String COMMON = "common";
    private static final String ARG_SYMBOL = "C";
    private static final String MINUS = "-";
    private static final String EQUAL = "=";
    private static final String SPACE = " ";

    public static final String MAX_STEPS = "max_steps";
    public static final int DEFAULT_MAX_STEPS = 5;
    public static final String PRECISION = "precision";
    public static final double DEFAULT_PRECISION = 0.0001D;

    protected static final String CATEGORY_RANK = "rank";
    protected static final String CATEGORY_COMM = "community";

    private YamlConfiguration config;
    private Map<String, Object> commonConfig = new HashMap<>();

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        E.checkArgument(parameters.isEmpty(),
                        "Unnecessary parameters: %s", parameters);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {

        this.checkAndCollectParameters(parameters);
        // Read configuration
        try {
            this.initializeConfig((ComputerJob) job);
        } catch (Exception e) {
            throw new HugeException(
                      "Failed to initialize computer config file", e);
        }

        // Set current computer job's specified parameters
        Map<String, Object> configs = new HashMap<>();
        configs.putAll(this.commonConfig);
        configs.putAll(this.checkAndCollectParameters(parameters));

        // Construct shell command for computer job
        String command = this.constructShellCommands(configs);

        // Execute current computer
        int exitCode;
        try {
            Process process = Runtime.getRuntime().exec(command);
            exitCode = process.waitFor();
        } catch (Throwable e) {
            throw new HugeException("Failed to execute computer job", e);
        }

        return exitCode;
    }

    private void initializeConfig(ComputerJob job) throws Exception {
        // Load computer config file
        String configPath = job.computerConfig();
        E.checkArgument(configPath.endsWith(".yaml"),
                        "Expect a yaml config file.");

        this.config = new YamlConfiguration();
        this.config.load(configPath);

        // Read common and computer specified parameters
        this.commonConfig = this.readCommonConfig();
    }

    private Map<String, Object> readCommonConfig() {
        return this.readSubConfig(COMMON);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readSubConfig(String sub) {
        List<ConfigurationNode> nodes = this.config.getRootNode()
                                                   .getChildren(sub);
        E.checkArgument(nodes.size() == 1,
                        "Must contain one '%s' node in config file '%s'",
                        sub, this.config.getFileName());

        List<ConfigurationNode> subConfigs = nodes.get(0).getChildren();
        Map<String, Object> results = new HashMap<>(subConfigs.size());
        for (ConfigurationNode node : subConfigs) {
            results.put(node.getName(), node.getValue());
        }

        return results;
    }

    @SuppressWarnings("unchecked")
    private String constructShellCommands(Map<String, Object> configs) {
        StringBuilder builder = new StringBuilder(1024);
        builder.append(COMMAND_PREFIX).append(SPACE)
               .append(this.name()).append(SPACE);
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            builder.append(MINUS).append(ARG_SYMBOL).append(SPACE)
                   .append(entry.getKey()).append(EQUAL)
                   .append(entry.getValue()).append(SPACE);
        }
        return builder.toString();
    }

    public abstract Map<String, Object> checkAndCollectParameters(
                                        Map<String, Object> parameters);

    protected static int maxSteps(Map<String, Object> parameters) {
        if (!parameters.containsKey(MAX_STEPS)) {
            return DEFAULT_MAX_STEPS;
        }
        int maxSteps = ParameterUtil.parameterInt(parameters, MAX_STEPS);
        E.checkArgument(maxSteps > 0,
                        "The value of %s must be > 0, but got %s",
                        MAX_STEPS, maxSteps);
        return maxSteps;
    }

    protected static double precision(Map<String, Object> parameters) {
        if (!parameters.containsKey(PRECISION)) {
            return DEFAULT_PRECISION;
        }
        double precision = ParameterUtil.parameterDouble(parameters, PRECISION);
        E.checkArgument(precision > 0.0D && precision < 1.0D,
                        "The value of %s must be (0, 1), but got %s",
                        PRECISION, precision);
        return precision;
    }
}
