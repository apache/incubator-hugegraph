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

package com.baidu.hugegraph;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.auth.UserManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.GraphMode;

/**
 * Graph interface for Gremlin operations
 */
public interface GremlinGraph extends Graph {

    public HugeGraph hugegraph();
    public HugeGraph hugegraph(String permission);

    public SchemaManager schema();

    public PropertyKey propertyKey(String key);
    public PropertyKey propertyKey(Id key);

    public VertexLabel vertexLabel(String label);
    public VertexLabel vertexLabel(Id label);

    public EdgeLabel edgeLabel(String label);
    public EdgeLabel edgeLabel(Id label);

    public IndexLabel indexLabel(String name);
    public IndexLabel indexLabel(Id id);

    public Iterator<Vertex> vertices(Query query);
    public Iterator<Edge> edges(Query query);
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) ;
    public Iterator<Edge> adjacentEdges(Id vertexId);

    public Number queryNumber(Query query);

    public String matchUser(String username, String password);
    public UserManager userManager();

    public String name();
    public String backend();
    public GraphMode mode();

    public <R> R metadata(HugeType type, String meta, Object... args);

    public void initBackend();
    public void clearBackend();
    public void truncateBackend();
}
