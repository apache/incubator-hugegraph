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

package com.baidu.hugegraph.api;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.core.MediaType;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.api.schema.Checkable;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

public class API {

    public static final String CHARSET = "UTF-8";

    public static final String APPLICATION_JSON = MediaType.APPLICATION_JSON;
    public static final String APPLICATION_JSON_WITH_CHARSET =
                               APPLICATION_JSON + ";charset=" + CHARSET;;

    public static final String ACTION_APPEND = "append";
    public static final String ACTION_ELIMINATE = "eliminate";

    public static Graph graph(GraphManager manager, String graph) {
        Graph g = manager.graph(graph);
        if (g == null) {
            String msg = String.format("Not found graph '%s'", graph);
            throw new NotFoundException(msg);
        }
        return g;
    }

    public static Object[] properties(Map<String, Object> properties) {
        Object[] list = new Object[properties.size() * 2];
        int i = 0;
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            list[i++] = prop.getKey();
            list[i++] = prop.getValue();
        }
        return list;
    }

    protected static void checkExist(Iterator<?> iter,
                                     HugeType type,
                                     String id) {
        if (!iter.hasNext()) {
            String msg = String.format("Not found the %s with id '%s'",
                                       type, id);
            throw new NotFoundException(msg);
        }
    }

    protected static void checkBody(Checkable body) {
        E.checkArgumentNotNull(body, "The request body can't be empty");
        body.check(false);
    }

    protected static void checkBody(Collection<? extends Checkable> bodys) {
        E.checkArgumentNotNull(bodys, "The request body can't be empty");
        for (Checkable body : bodys) {
            body.check(true);
        }
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> parseProperties(String properties) {
        if (properties == null || properties.isEmpty()) {
            return ImmutableMap.of();
        }
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> props = null;
        try {
            props = mapper.readValue(properties, Map.class);
        } catch (Exception ignore) {
        }
        // If properties is the string "null", props will be null
        if (props == null) {
            throw new IllegalArgumentException(String.format(
                      "Invalid request with properties: %s", properties));
        }
        return props;
    }

    public static boolean checkAndParseAction(String action) {
        E.checkArgumentNotNull(action, "The action param can't be empty");
        if (action.equals(ACTION_APPEND)) {
            return true;
        } else if (action.equals(ACTION_ELIMINATE)) {
            return false;
        } else {
            throw new NotSupportedException(
                      String.format("Not support action '%s'", action));
        }
    }
}
