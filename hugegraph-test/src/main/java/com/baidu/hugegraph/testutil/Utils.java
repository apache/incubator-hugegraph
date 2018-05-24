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

package com.baidu.hugegraph.testutil;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.io.HugeGraphSONModule;
import com.baidu.hugegraph.testutil.FakeObjects.FakeEdge;
import com.baidu.hugegraph.testutil.FakeObjects.FakeVertex;

public class Utils {

    public static final String CONF_PATH = "hugegraph.properties";

    public static HugeGraph open() {
        String confPath = System.getProperty("config_path");
        if (confPath == null || confPath.isEmpty()) {
            confPath = CONF_PATH;
        }
        try {
            confPath = Utils.class.getClassLoader()
                            .getResource(confPath).getPath();
        } catch (Exception ignored) {
        }
        return HugeFactory.open(confPath);
    }

    public static boolean containsId(List<Vertex> vertexes, Id id) {
        for (Vertex v : vertexes) {
            if (v.id().equals(id)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(List<Vertex> vertexes,
                                   FakeVertex fakeVertex) {
        for (Vertex v : vertexes) {
            if (fakeVertex.equalsVertex(v)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(List<Edge> edges, FakeEdge fakeEdge) {
        for (Edge e : edges) {
            if (fakeEdge.equalsEdge(e)) {
                return true;
            }
        }
        return false;
    }

    public static Date date(String rawDate) {
        try {
            return HugeGraphSONModule.DATE_FORMAT.parse(rawDate);
        } catch (ParseException e) {
            throw new IllegalArgumentException(String.format(
                      "Invalid date '%s'", rawDate));
        }
    }
}
