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

package com.baidu.hugegraph.auth;

import com.baidu.hugegraph.auth.SchemaDefine.AuthElement;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.util.E;

public class ResourceObject<V> {

    private final String graphSpace;
    private final String graph;
    private final ResourceType type;
    private final V operated;

    public ResourceObject(String graphSpace, String graph,
                          ResourceType type, V operated) {
        E.checkNotNull(graphSpace, "graphSpace");
        E.checkNotNull(graph, "graph");
        E.checkNotNull(type, "type");
        E.checkNotNull(operated, "operated");
        this.graphSpace = graphSpace;
        this.graph = graph;
        this.type = type;
        this.operated = operated;
    }

    public String graphSpace() {
        return this.graphSpace;
    }

    public String graph() {
        return this.graph;
    }

    public ResourceType type() {
        return this.type;
    }

    public V operated() {
        return this.operated;
    }

    @Override
    public String toString() {
        Object operated = this.operated;
        if (this.type.isAuth()) {
            operated = ((AuthElement) this.operated).idString();
        }

        String typeStr = this.type.toString();
        String operatedStr = operated.toString();
        int capacity = this.graph.length() + typeStr.length() +
                       operatedStr.length() + 36;

        StringBuilder sb = new StringBuilder(capacity);
        return sb.append("Resource{graphspace=").append(this.graphSpace)
                 .append(",graph=").append(this.graph)
                 .append(",type=").append(typeStr)
                 .append(",operated=").append(operatedStr)
                 .append("}").toString();
    }

    public static ResourceObject<SchemaElement> of(String graphSpace,
                                                   String graph,
                                                   SchemaElement elem) {
        ResourceType resType = ResourceType.from(elem.type());
        return new ResourceObject<>(graphSpace, graph, resType, elem);
    }

    public static ResourceObject<HugeElement> of(String graphSpace,
                                                 String graph,
                                                 HugeElement elem) {
        ResourceType resType = ResourceType.from(elem.type());
        return new ResourceObject<>(graphSpace, graph, resType, elem);
    }

    public static ResourceObject<AuthElement> of(String graphSpace,
                                                 String graph,
                                                 AuthElement elem) {
        return new ResourceObject<>(graphSpace, graph, elem.type(), elem);
    }

    public static ResourceObject<?> of(String graphSpace, String graph,
                                       ResourceType type, Namifiable elem) {
        return new ResourceObject<>(graphSpace, graph, type, elem);
    }
}
