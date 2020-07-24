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

package com.baidu.hugegraph.job.compute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.job.ComputeJob;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.util.E;

public abstract class AbstractCompute implements Compute {

    private static final String COMMAND_PREFIX =
            "cd $COMPUTER_HOME;" +
            "hadoop jar hugegraph-computer.jar com.baidu.hugegraph.Computer " +
            "-D libjars=./hugegraph-computer-core.jar";

    private static final String COMMON = "common";
    private static final String EXTRA_ARGS = "extra_args";
    private static final String MINUS = "-";
    private static final String EQUAL = "=";
    private static final String EMPTY = " ";

    public static final String MAX_STEPS = "max_steps";
    public static final int DEFAULT_MAX_STEPS = 5;

    protected static final String CATEGORY_RANK = "rank";
    protected static final String CATEGORY_COMM = "community";

    private YamlConfiguration config;
    private Map<String, Object> commonConfig = new HashMap<>();
    protected Map<String, Object> userDefinedParameters = new HashMap<>();

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        E.checkArgument(parameters.isEmpty(),
                        "Unnecessary parameters: %s", parameters);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        // Read configuration
        try {
            this.initializeConfig((ComputeJob) job);
        } catch (Exception e) {
            throw new HugeException(
                      "Failed to initialize computer config file", e);
        }

        // Set current compute job's specified parameters
        this.setComputeSpecifiedParameters();

        // Construct shell command for compute job
        String command = constructShellCommands(this.commonConfig);

        // Execute current compute
        int exitCode;
        try {
            Process process = Runtime.getRuntime().exec(command);
            exitCode = process.waitFor();
        } catch (Exception e) {
            throw new HugeException("Failed to execute compute job", e);
        }

        return exitCode;
    }

    private void initializeConfig(ComputeJob job) throws Exception {
        // Load computer config file
        String configPath = job.config().get(CoreOptions.COMPUTER_CONFIG);
        E.checkArgument(configPath.endsWith(".yaml"),
                        "Expect a yaml config file.");

        this.config = new YamlConfiguration();
        this.config.load(configPath);

        // Read common and compute specified parameters
        this.commonConfig = this.readCommonConfig();
    }

    private Map<String, Object> readCommonConfig() {
        return readSubConfig(COMMON);
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
            if (!node.getName().equals(EXTRA_ARGS)) {
                results.put(node.getName(), node.getValue());
                continue;
            }
            assert node.getName().equals(EXTRA_ARGS);
            List<ConfigurationNode> extraNodes = node.getChildren();
            Map<String, Object> extras = new HashMap<>(extraNodes.size());
            for (ConfigurationNode n : extraNodes) {
                String key = ((Map.Entry<String, Object>)
                             n.getReference()).getKey();
                extras.put(key, n.getValue());
            }
            results.put(node.getName(), extras);
        }

        return results;
    }

    @SuppressWarnings("unchecked")
    private void setComputeSpecifiedParameters() {
        ((Map<String, Object>) this.commonConfig.get(EXTRA_ARGS))
                               .putAll(this.userDefinedParameters);
    }

    @SuppressWarnings("unchecked")
    private String constructShellCommands(Map<String, Object> configs) {
        StringBuilder builder = new StringBuilder(1024);
        builder.append(COMMAND_PREFIX).append(EMPTY)
               .append(this.name()).append(EMPTY);
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            if (!entry.getKey().equals(EXTRA_ARGS)) {
                builder.append(MINUS).append(entry.getKey()).append(EMPTY)
                       .append(entry.getValue()).append(EMPTY);
                continue;
            }
            Map<String, Object> extras = (Map<String, Object>) entry.getValue();
            for (Map.Entry<String, Object> extra : extras.entrySet()) {
                builder.append(MINUS).append(EXTRA_ARGS).append(EMPTY)
                       .append(extra.getKey()).append(EQUAL)
                       .append(extra.getValue()).append(EMPTY);
            }
        }
        return builder.toString();
    }

    protected static int maxSteps(Map<String, Object> parameters) {
        if (!parameters.containsKey(MAX_STEPS)) {
            return DEFAULT_MAX_STEPS;
        }
        int maxSteps = parameterInt(parameters, MAX_STEPS);
        E.checkArgument(maxSteps > 0,
                        "The value of %s must be > 0, but got %s",
                        MAX_STEPS, maxSteps);
        return maxSteps;
    }

    public static Object parameter(Map<String, Object> parameters, String key) {
        Object value = parameters.get(key);
        E.checkArgument(value != null,
                        "Expect '%s' in parameters: %s",
                        key, parameters);
        return value;
    }

    public static String parameterString(Map<String, Object> parameters,
                                         String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof String,
                        "Expect string value for parameter '%s': '%s'",
                        key, value);
        return (String) value;
    }

    public static int parameterInt(Map<String, Object> parameters,
                                   String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect int value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).intValue();
    }

    public static long parameterLong(Map<String, Object> parameters,
                                     String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect long value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).longValue();
    }

    public static double parameterDouble(Map<String, Object> parameters,
                                         String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect double value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).doubleValue();
    }

    public static boolean parameterBoolean(Map<String, Object> parameters,
                                           String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Boolean,
                        "Expect boolean value for parameter '%s': '%s'",
                        key, value);
        return ((Boolean) value);
    }
}
