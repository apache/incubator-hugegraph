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

package com.baidu.hugegraph.base;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.google.common.base.Preconditions;

public class HugeGraphProvider extends AbstractGraphProvider {

    public static String CONF_PATH = "hugegraph.properties";

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {
        {
            add(HugeEdge.class);
            add(HugeElement.class);
            add(HugeGraph.class);
            add(HugeProperty.class);
            add(HugeVertex.class);
            add(HugeVertexProperty.class);
        }
    };

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName,
            Class<?> aClass, String s1, LoadGraphWith.GraphData graphData) {
        HashMap<String, Object> confMap = new HashMap<>();
        String confFile = HugeGraphProvider.class.getClassLoader()
                .getResource(CONF_PATH).getPath();
        File file = new File(confFile);
        PropertiesConfiguration config;
        Preconditions.checkArgument(
                file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable configuration file rather than: %s",
                file.toString());
        try {
            config = new PropertiesConfiguration(file);
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load config file: %s", e,
                    confFile);
        }
        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            confMap.put(key, config.getProperty(key));
        }
        confMap.put(CoreOptions.STORE.name(), graphName);

        return confMap;
    }

    @Override
    public Graph openTestGraph(final Configuration config) {
        HugeGraph graph = (HugeGraph) super.openTestGraph(config);

        graph.clearBackend();
        graph.initBackend();

        graph.tx().open();
        return new TestGraph(graph);
    }

    @Override
    public void clear(Graph graph, Configuration configuration)
            throws Exception {
        if (graph != null && graph.tx().isOpen()) {
            ((TestGraph) graph).hugeGraph().clearBackend();
            ((TestGraph) graph).hugeGraph().close();
        }
    }

    @Override
    public void loadGraphData(final Graph graph,
                              final LoadGraphWith loadGraphWith,
                              final Class testClass,
                              final String testName) {
        if (loadGraphWith != null) {
            ((TestGraph) graph).loadedGraph(true);
            switch (loadGraphWith.value()) {
                case GRATEFUL:
                    initGratefulSchema(graph);
                    break;
                case MODERN:
                    initModernSchema(graph);
                    break;
                case CLASSIC:
                    initClassicSchema(graph);
                    break;
                default:
                    throw new AssertionError(String.format(
                            "Only support GRATEFUL, MODERN and CLASSIC for "
                                    + "@LoadGraphWith(), but '%s' is used ",
                            loadGraphWith.value()));
            }
        } else {
            initBasicSchema(graph);
        }
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }

    public static void initGratefulSchema(final Graph graph) {
        SchemaManager schema = ((TestGraph) graph).hugeGraph().schema();

        schema.makePropertyKey("id").asInt().ifNotExist().create();
        schema.makePropertyKey("weight").asInt().ifNotExist().create();
        schema.makePropertyKey("name").ifNotExist().create();
        schema.makePropertyKey("songType").asText().ifNotExist()
                .create();
        schema.makePropertyKey("performances").asInt().ifNotExist()
                .create();
        schema.makeVertexLabel("song")
                .properties("id", "name", "songType", "performances")
                .primaryKeys("id").ifNotExist().create();
        schema.makeVertexLabel("artist").properties("id", "name")
                .primaryKeys("id").ifNotExist().create();
        schema.makeEdgeLabel("followedBy")
                .link("song", "song").properties("weight")
                .ifNotExist().create();
        schema.makeEdgeLabel("sungBy").link("song", "artist")
                .ifNotExist().create();
        schema.makeEdgeLabel("writtenBy").link("song", "artist")
                .ifNotExist().create();
    }

    public static void initModernSchema(final Graph graph) {
        SchemaManager schema = ((TestGraph) graph).hugeGraph().schema();

        schema.makePropertyKey("id").asInt().ifNotExist().create();
        schema.makePropertyKey("weight").asDouble().ifNotExist().create();
        schema.makePropertyKey("name").ifNotExist().create();
        schema.makePropertyKey("lang").ifNotExist().create();
        schema.makePropertyKey("age").asInt().ifNotExist().create();

        schema.makeVertexLabel("person").properties("id", "name", "age")
                .primaryKeys("id").ifNotExist().create();
        schema.makeVertexLabel("software").properties("id", "name", "lang")
                .primaryKeys("id").ifNotExist().create();
        schema.makeEdgeLabel("knows").link("person", "person")
                .properties("weight").ifNotExist().create();
        schema.makeEdgeLabel("created").link("person", "software")
                .properties("weight").ifNotExist().create();
    }

    public static void initClassicSchema(final Graph graph) {
        SchemaManager schema = ((TestGraph) graph).hugeGraph().schema();

        schema.makePropertyKey("id").asInt().ifNotExist().create();
        schema.makePropertyKey("weight").asFloat().ifNotExist().create();
        schema.makePropertyKey("name").ifNotExist().create();
        schema.makePropertyKey("lang").ifNotExist().create();
        schema.makePropertyKey("age").asInt().ifNotExist().create();

        schema.makeVertexLabel("vertex")
                .properties("id", "name", "age", "lang")
                .primaryKeys("id").ifNotExist().create();
        schema.makeEdgeLabel("knows").link("vertex", "vertex")
                .properties("weight").ifNotExist().create();
        schema.makeEdgeLabel("created").link("vertex", "vertex")
                .properties("weight").ifNotExist().create();
    }

    public static void initBasicSchema(final Graph graph) {
        SchemaManager schema = ((TestGraph) graph).hugeGraph().schema();

        schema.makePropertyKey("oid").asInt().ifNotExist().create();
        schema.makePropertyKey("__id").asText().ifNotExist().create();
        schema.makePropertyKey("communityIndex").asInt()
                .ifNotExist().create();
        schema.makePropertyKey("test").ifNotExist().create();
        schema.makePropertyKey("data").ifNotExist().create();
        schema.makePropertyKey("name").ifNotExist().create();
        schema.makePropertyKey("location").ifNotExist().create();
        schema.makePropertyKey("status").ifNotExist().create();
        schema.makePropertyKey("boolean").asBoolean()
                .ifNotExist().create();
        schema.makePropertyKey("float").asFloat().ifNotExist().create();
        schema.makePropertyKey("since").asInt().ifNotExist().create();
        schema.makePropertyKey("double").asDouble().ifNotExist().create();
        schema.makePropertyKey("string").ifNotExist().create();
        schema.makePropertyKey("integer").asInt().ifNotExist().create();
        schema.makePropertyKey("long").asLong().ifNotExist().create();
        schema.makePropertyKey("x").asInt().ifNotExist().create();
        schema.makePropertyKey("y").asInt().ifNotExist().create();
        schema.makePropertyKey("aKey").asDouble().ifNotExist().create();
        schema.makePropertyKey("age").asInt().ifNotExist().create();
        schema.makePropertyKey("lang").ifNotExist().create();
        schema.makePropertyKey("weight").asDouble().ifNotExist().create();
        schema.makePropertyKey("some").ifNotExist().create();
        schema.makePropertyKey("that").ifNotExist().create();
        schema.makePropertyKey("any").ifNotExist().create();
        schema.makePropertyKey("this").ifNotExist().create();
        schema.makePropertyKey("communityIndex").ifNotExist().create();


        schema.makeVertexLabel("v").properties("__id", "oid", "name",
                "some", "that", "any", "this")
                .primaryKeys("__id").ifNotExist().create();
        schema.makeEdgeLabel("self").link("v", "v")
                .properties("__id", "test", "name", "some")
                .ifNotExist()
                .create();
        schema.makeEdgeLabel("aTOa").link("v", "v")
                .ifNotExist().create();
        schema.makeEdgeLabel("connectsTo").link("v", "v")
                .ifNotExist().create();

        // schema.makeEdgeLabel("collaborator").ifNotExist().create();
        // schema.makeEdgeLabel("knows").ifNotExist().create();
        // schema.makeEdgeLabel("friend").ifNotExist().create();
        // schema.makeEdgeLabel("hate").ifNotExist().create();
        // schema.makeEdgeLabel("test1").ifNotExist().create();
        // schema.makeEdgeLabel("link").ifNotExist().create();
        // schema.makeEdgeLabel("test2").ifNotExist().create();
        // schema.makeEdgeLabel("test3").ifNotExist().create();
        // schema.makeEdgeLabel("self").ifNotExist().create();
        // schema.makeEdgeLabel("~systemLabel").ifNotExist().create();
        // schema.makeEdgeLabel("friends").ifNotExist().create();
        // schema.makeEdgeLabel("l").ifNotExist().create();
        // schema.makeEdgeLabel("created").ifNotExist().create();
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @Override
    public GraphTraversalSource traversal(Graph graph) {
        return ((TestGraph) graph).hugeGraph().traversal();
    }

    static class TestGraph implements Graph {

        protected static int id = 0;
        protected HugeGraph graph;
        private boolean loadedGraph = false;

        public TestGraph(HugeGraph graph) {
            this.graph = graph;
        }

        public HugeGraph hugeGraph() {
            return this.graph;
        }

        @Override
        public Vertex addVertex(Object... keyValues) {
            List<Object> kvs = new ArrayList<>();
            Optional<Object> idValue = ElementHelper.getIdValue(keyValues);
            if (!idValue.isPresent()) {
                kvs.add(loadedGraph ? "id" : "__id");
                kvs.add(loadedGraph ? id : String.valueOf(id));
                id++;
            }

            for (int i = 0; i < keyValues.length; i += 2) {
                kvs.add(keyValues[i].equals(T.id)
                        ? (loadedGraph ? "id" : "__id") : keyValues[i]);
                kvs.add(keyValues[i + 1]);
            }

            return this.graph.addVertex(kvs.toArray());

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
    }
}
