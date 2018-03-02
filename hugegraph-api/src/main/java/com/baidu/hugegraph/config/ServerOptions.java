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

package com.baidu.hugegraph.config;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

public class ServerOptions extends OptionHolder {

    private ServerOptions() {
        super();
    }

    private static volatile ServerOptions instance;

    public static ServerOptions instance() {
        if (instance == null) {
            synchronized (ServerOptions.class) {
                if (instance == null) {
                    instance = new ServerOptions();
                    instance.registerOptions();
                }
            }
        }
        return instance;
    }

    public static final ConfigOption<String> REST_SERVER_URL =
            new ConfigOption<>(
                    "restserver.url",
                    "The url for listening of hugeserver.",
                    disallowEmpty(),
                    "http://127.0.0.1:8080"
            );

    public static final ConfigOption<String> GREMLIN_SERVER_URL =
            new ConfigOption<>(
                    "gremlinserver.url",
                    "The url of gremlin server.",
                    disallowEmpty(),
                    "http://127.0.0.1:8182"
            );

    public static final ConfigListOption<String> GRAPHS =
            new ConfigListOption<>(
                    "graphs",
                    "The map of graphs' name and config file.",
                    disallowEmpty(),
                    "hugegraph:conf/hugegraph.properties"
            );

    public static final ConfigOption<Integer> MAX_VERTICES_PER_BATCH =
            new ConfigOption<>(
                    "max_vertices_per_batch",
                    "The maximum number of vertices submitted per batch.",
                    positiveInt(),
                    500
            );

    public static final ConfigOption<Integer> MAX_EDGES_PER_BATCH =
            new ConfigOption<>(
                    "max_edges_per_batch",
                    "The maximum number of edges submitted per batch.",
                    positiveInt(),
                    500
            );

    public static final ConfigOption<String> ADMIN_TOKEN =
            new ConfigOption<>(
                    "admin.token",
                    "Token for administrator operations",
                    disallowEmpty(),
                    "162f7848-0b6d-4faf-b557-3a0797869c55"
            );
}
