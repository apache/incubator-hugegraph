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

package com.baidu.hugegraph.space;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.baidu.hugegraph.util.E;

public class GraphSpace {

    public static final String DEFAULT_GRAPH_SPACE_NAME = "DEFAULT";

    private static final int DEFAULT_MAX_GRAPH_NUMBER = 100;
    private static final int DEFAULT_MAX_ROLE_NUMBER = 100;
    private static final String MAX_GRAPH_NUMBER = "max_graph_number";
    private static final String MAX_ROLE_NUMBER = "max_role_number";

    private final String name;
    private final int maxGraphNumber;
    private final int maxRoleNumber;
    private final Map<String, Object> config;

    public GraphSpace(String name) {
        this.name = name;
        this.maxGraphNumber = DEFAULT_MAX_GRAPH_NUMBER;
        this.maxRoleNumber = DEFAULT_MAX_ROLE_NUMBER;
        this.config = new HashMap<>();
    }

    public GraphSpace(String name, int maxGraphNumber, int maxRoleNumber,
                      Map<String, Object> config) {
        E.checkArgument(name != null && !StringUtils.isEmpty(name),
                        "The name of graph space can't be null or empty");
        E.checkArgument(maxGraphNumber > 0, "The max graph number must > 0");
        E.checkArgument(maxRoleNumber > 0, "The max role number must > 0");
        this.name = name;
        this.maxGraphNumber = maxGraphNumber;
        this.maxRoleNumber = maxRoleNumber;

        this.config = config;
    }

    public String name() {
        return this.name;
    }

    public int maxGraphNumber() {
        return this.maxGraphNumber;
    }

    public int maxRoleNumber() {
        return this.maxRoleNumber;
    }

    public Map<String, Object> info() {
        Map<String, Object> infos = new LinkedHashMap<>();
        infos.put("name", this.name);
        infos.put(MAX_GRAPH_NUMBER, this.maxGraphNumber);
        infos.put(MAX_ROLE_NUMBER, this.maxRoleNumber);
        infos.putAll(this.config);
        return infos;
    }
}
