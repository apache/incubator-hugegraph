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

package com.baidu.hugegraph.tinkerpop;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.type.define.IdStrategy;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public class TestGraph implements Graph {

    private HugeGraph graph;
    private boolean loadedGraph = false;
    public String defaultVL = "vertex";

    public TestGraph(HugeGraph graph) {
        this.graph = graph;
    }

    public HugeGraph hugeGraph() {
        return this.graph;
    }

    protected void clearSchema() {
        // Clear schema and graph data will be cleared at same time
        SchemaTransaction schema = this.graph.schemaTransaction();

        schema.getIndexLabels().stream().forEach(elem -> {
            schema.removeIndexLabel(elem.name());
        });

        schema.getEdgeLabels().stream().forEach(elem -> {
            schema.removeEdgeLabel(elem.name());
        });

        schema.getVertexLabels().stream().forEach(elem -> {
            schema.removeVertexLabel(elem.name());
        });

        schema.getPropertyKeys().stream().forEach(elem -> {
            schema.removePropertyKey(elem.name());
        });
    }

    @Override
    public Vertex addVertex(Object... keyValues) {

        for (Object obj : keyValues) {
            if (obj.equals(T.id)) {
                this.clearSchema();
                this.tx().commit();
                this.initBasicSchema(IdStrategy.CUSTOMIZE);
                this.tx().commit();
                break;
            }
        }

        return this.graph.addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass)
            throws IllegalArgumentException {
        return this.graph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return this.graph.compute();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return this.graph.vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (this.graph.tx().isOpen()) {
            this.graph.tx().commit();
        }
        return this.graph.edges(edgeIds);
    }

    @Override
    public Transaction tx() {
        return this.graph.tx();
    }

    @Override
    public void close() throws Exception {
        this.graph.close();
    }

    @Override
    public Variables variables() {
        return this.graph.variables();
    }

    @Override
    public Configuration configuration() {
        return this.graph.configuration();
    }

    @Override
    public HugeFeatures features() {
        return this.graph.features();
    }

    public void loadedGraph(boolean loadedGraph) {
        this.loadedGraph = loadedGraph;
    }

    public void initGratefulSchema() {
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("id").asInt().ifNotExist().create();
        schema.propertyKey("weight").asInt().ifNotExist().create();
        schema.propertyKey("name").ifNotExist().create();
        schema.propertyKey("songType").asText().ifNotExist().create();
        schema.propertyKey("performances").asInt().ifNotExist().create();

        schema.vertexLabel("song")
              .properties("id", "name", "songType", "performances")
              .ifNotExist().create();
        schema.vertexLabel("artist").properties("id", "name")
              .ifNotExist().create();

        schema.edgeLabel("followedBy")
              .link("song", "song").properties("weight")
              .ifNotExist().create();
        schema.edgeLabel("sungBy").link("song", "artist")
              .ifNotExist().create();
        schema.edgeLabel("writtenBy").link("song", "artist")
              .ifNotExist().create();
    }

    public void initModernSchema() {
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("id").asInt().ifNotExist().create();
        schema.propertyKey("weight").asDouble().ifNotExist().create();
        schema.propertyKey("name").ifNotExist().create();
        schema.propertyKey("lang").ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("year").asInt().ifNotExist().create();

        schema.vertexLabel("person").properties("id", "name", "age")
              .ifNotExist().create();
        schema.vertexLabel("software").properties("id", "name", "lang")
              .ifNotExist().create();
        schema.vertexLabel("dog").properties("name").ifNotExist().create();
        schema.vertexLabel("vertex").properties("name", "age")
              .ifNotExist().create();

        schema.edgeLabel("knows").link("person", "person")
              .properties("weight", "year").ifNotExist().create();
        schema.edgeLabel("created").link("person", "software")
              .properties("weight").ifNotExist().create();
    }

    public void initClassicSchema() {
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("id").asInt().ifNotExist().create();
        schema.propertyKey("weight").asFloat().ifNotExist().create();
        schema.propertyKey("name").ifNotExist().create();
        schema.propertyKey("lang").ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();

        schema.vertexLabel("vertex").properties("id", "name", "age", "lang")
              .ifNotExist().create();

        schema.edgeLabel("knows").link("vertex", "vertex")
              .properties("weight").ifNotExist().create();
        schema.edgeLabel("created").link("vertex", "vertex")
              .properties("weight").ifNotExist().create();
    }

    public void initBasicSchema(IdStrategy idStrategy) {
        this.initBasicPropertyKey();
        this.initBasicVertexLabelV(idStrategy);
        this.initBasicVertexLabelAndEdgeLabelExceptV();
    }

    private void initBasicPropertyKey() {
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("oid").asInt().ifNotExist().create();
        schema.propertyKey("__id").asText().ifNotExist().create();
        schema.propertyKey("communityIndex").asInt().ifNotExist().create();
        schema.propertyKey("test").ifNotExist().create();
        schema.propertyKey("testing").ifNotExist().create();
        schema.propertyKey("data").ifNotExist().create();
        schema.propertyKey("name").ifNotExist().create();
        schema.propertyKey("location").ifNotExist().create();
        schema.propertyKey("status").ifNotExist().create();
        schema.propertyKey("boolean").asBoolean().ifNotExist().create();
        schema.propertyKey("float").asFloat().ifNotExist().create();
        schema.propertyKey("since").asInt().ifNotExist().create();
        schema.propertyKey("double").asDouble().ifNotExist().create();
        schema.propertyKey("string").ifNotExist().create();
        schema.propertyKey("integer").asInt().ifNotExist().create();
        schema.propertyKey("long").asLong().ifNotExist().create();
        schema.propertyKey("x").asInt().ifNotExist().create();
        schema.propertyKey("y").asInt().ifNotExist().create();
        schema.propertyKey("aKey").asDouble().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("lang").ifNotExist().create();
        schema.propertyKey("weight").asDouble().ifNotExist().create();
        schema.propertyKey("some").ifNotExist().create();
        schema.propertyKey("that").ifNotExist().create();
        schema.propertyKey("any").ifNotExist().create();
        schema.propertyKey("this").ifNotExist().create();
        schema.propertyKey("year").asInt().ifNotExist().create();
        schema.propertyKey("xxx").ifNotExist().create();
        schema.propertyKey("yyy").ifNotExist().create();
        schema.propertyKey("favoriteColor").ifNotExist().create();
        schema.propertyKey("uuid").asUuid().ifNotExist().create();
        schema.propertyKey("myId").asInt().ifNotExist().create();
        schema.propertyKey("myEdgeId").asInt().ifNotExist().create();
        schema.propertyKey("state").ifNotExist().create();
        schema.propertyKey("acl").ifNotExist().create();

    }

    private void initBasicVertexLabelV(IdStrategy idStrategy) {
        SchemaManager schema = this.graph.schema();
        switch (idStrategy) {
            case CUSTOMIZE:
                schema.vertexLabel(defaultVL)
                      .properties("__id", "oid", "name", "state", "status",
                                  "some", "that", "any", "this",
                                  "communityIndex", "test", "testing",
                                  "favoriteColor", "aKey", "age", "boolean",
                                  "float", "double", "string", "integer",
                                  "long", "myId", "location")
                      .useCustomizeId().ifNotExist().create();
                break;
            case AUTOMATIC:
                schema.vertexLabel(defaultVL)
                      .properties("__id", "oid", "name", "state", "status",
                                  "some", "that", "any", "this",
                                  "communityIndex", "test", "testing",
                                  "favoriteColor", "aKey", "age", "boolean",
                                  "float", "double", "string", "integer",
                                  "long", "myId", "location")
                      .ifNotExist().create();
                break;
            default:
                throw new AssertionError("Only customize and automatic " +
                                         "is legal in tinkerpop tests");
        }
    }

    private void initBasicVertexLabelAndEdgeLabelExceptV() {
        SchemaManager schema = this.graph.schema();

        schema.vertexLabel("person").properties("name")
              .ifNotExist().create();

        schema.edgeLabel("self").link(defaultVL, defaultVL)
              .properties("__id", "test", "name", "some", "acl")
              .ifNotExist().create();
        schema.edgeLabel("aTOa").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("connectsTo").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("knows").link(defaultVL, defaultVL)
              .properties("data", "test", "year", "boolean", "float",
                          "double", "string", "integer", "long",
                          "myEdgeId", "since")
              .ifNotExist().create();
        schema.edgeLabel("test").link(defaultVL, defaultVL)
              .properties("test", "xxx", "yyy").ifNotExist().create();
        schema.edgeLabel("friend").link(defaultVL, defaultVL)
              .properties("name", "location", "status", "uuid", "weight")
              .ifNotExist().create();
        schema.edgeLabel("pets").link(defaultVL, defaultVL).ifNotExist().create();
        schema.edgeLabel("walks").link(defaultVL, defaultVL).properties("location")
              .ifNotExist().create();
        schema.edgeLabel("livesWith").link(defaultVL, defaultVL).ifNotExist().create();
        schema.edgeLabel("friends").link(defaultVL, defaultVL).ifNotExist().create();
        schema.edgeLabel("collaborator").link(defaultVL, defaultVL)
              .properties("location").ifNotExist().create();
        schema.edgeLabel("hate").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("test1").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("link").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("test2").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("test3").link(defaultVL, defaultVL)
              .ifNotExist().create();
        // schema.edgeLabel("self").ifNotExist().create();
        // schema.edgeLabel("~systemLabel").ifNotExist().create();
        // schema.edgeLabel("l").ifNotExist().create();
        // schema.edgeLabel("created").ifNotExist().create();
    }
}