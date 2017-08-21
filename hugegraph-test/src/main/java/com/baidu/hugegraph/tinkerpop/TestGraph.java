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
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeFeatures;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class TestGraph implements Graph {

    private static int id = 0;

    private HugeGraph graph;
    private boolean loadedGraph = false;

    public TestGraph(HugeGraph graph) {
        this.graph = graph;
    }

    public HugeGraph hugeGraph() {
        return this.graph;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
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
        schema.vertexLabel("v").properties("name", "age")
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

    public void initBasicSchema() {
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

        schema.vertexLabel("v").properties(
                "__id", "oid", "name", "state",
                "some", "that", "any", "this", "communityIndex", "test",
                "testing", "favoriteColor", "aKey", "age", "boolean", "float",
                "double", "string", "integer", "long", "myId")
              .ifNotExist().create();
        schema.vertexLabel("person").ifNotExist().create();

        schema.edgeLabel("self").link("v", "v")
              .properties("__id", "test", "name", "some").ifNotExist().create();
        schema.edgeLabel("aTOa").link("v", "v").ifNotExist().create();
        schema.edgeLabel("connectsTo").link("v", "v").ifNotExist().create();
        schema.edgeLabel("knows").link("v", "v")
              .properties("data", "test", "year", "boolean", "float",
                          "double", "string", "integer", "long",
                          "myEdgeId", "since")
              .ifNotExist().create();
        schema.edgeLabel("test").link("v", "v")
              .properties("test", "xxx", "yyy").ifNotExist().create();
        schema.edgeLabel("friend").link("v", "v")
              .properties("name", "location", "status", "uuid", "weight")
              .ifNotExist().create();
        schema.edgeLabel("pets").link("v", "v").ifNotExist().create();
        schema.edgeLabel("walks").link("v", "v").properties("location")
              .ifNotExist().create();
        schema.edgeLabel("livesWith").link("v", "v").ifNotExist().create();
        schema.edgeLabel("friends").link("v", "v").ifNotExist().create();
        // schema.edgeLabel("collaborator").ifNotExist().create();
        // schema.edgeLabel("hate").ifNotExist().create();
        // schema.edgeLabel("test1").ifNotExist().create();
        // schema.edgeLabel("link").ifNotExist().create();
        // schema.edgeLabel("test2").ifNotExist().create();
        // schema.edgeLabel("test3").ifNotExist().create();
        // schema.edgeLabel("self").ifNotExist().create();
        // schema.edgeLabel("~systemLabel").ifNotExist().create();
        // schema.edgeLabel("l").ifNotExist().create();
        // schema.edgeLabel("created").ifNotExist().create();
    }
}