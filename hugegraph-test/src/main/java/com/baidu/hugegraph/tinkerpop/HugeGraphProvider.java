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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Assume;
import org.slf4j.Logger;

public class HugeGraphProvider extends AbstractGraphProvider {

    private static final Logger LOG = Log.logger(HugeGraphProvider.class);

    private static String CONF_PATH = "hugegraph.properties";
    private static String FILETER_FILE = "methods.filter";
    private static Map<String, String> blackMethods = new HashMap<>();

    public HugeGraphProvider() throws IOException {
        super();
        this.initBlackList();
    }

    private void initBlackList() throws IOException {
        String blackList = HugeGraphProvider.class.getClassLoader()
                           .getResource(FILETER_FILE).getPath();
        File file = new File(blackList);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty() || line.startsWith("#")) {
                // Empty line or comment line
                continue;
            }
            String[] parts = line.split(":");
            Assert.assertEquals("methods.filter proper format is: " +
                                "'testMethodName: ignore reason'",
                                2, parts.length);
            Assert.assertTrue(
                   "Test method name in methods.filter can't be empty",
                   parts[0] != null && !parts[0].trim().isEmpty());
            Assert.assertTrue(
                   "The reason why ignore in methods.filter can't be empty",
                   parts[1] != null && !parts[1].trim().isEmpty());
            blackMethods.putIfAbsent(parts[0], parts[1]);
        }
    }

    @SuppressWarnings("rawtypes")
    private static final Set<Class> IMPLEMENTATIONS = ImmutableSet.of(
            HugeEdge.class,
            HugeElement.class,
            HugeGraph.class,
            HugeProperty.class,
            HugeVertex.class,
            HugeVertexProperty.class);

    @Override
    public Map<String, Object> getBaseConfiguration(
                               String graphName,
                               Class<?> test, String testMethod,
                               LoadGraphWith.GraphData graphData) {
        // Check if test in blackList
        String testFullName = test.getCanonicalName() + "." + testMethod;
        Assume.assumeFalse(
               String.format("Test %s will be ignored with reason: %s",
                             testFullName, blackMethods.get(testMethod)),
               blackMethods.containsKey(testFullName));

        LOG.info("Full name of test is: {}", testFullName);
        HashMap<String, Object> confMap = new HashMap<>();
        String confFile = HugeGraphProvider.class.getClassLoader()
                                           .getResource(CONF_PATH).getPath();
        File file = new File(confFile);
        E.checkArgument(
                file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable configuration file rather than: %s",
                file.toString());

        PropertiesConfiguration config;
        try {
            config = new PropertiesConfiguration(file);
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load config file: %s",
                                    e, confFile);
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

        TestGraph testGraph = new TestGraph(graph);
        // Basic schema is initiated by default once a graph is open
        testGraph.initBasicSchema();
        testGraph.tx().commit();

        return testGraph;
    }

    protected void clearSchemaData(TestGraph testGraph) {
        HugeGraph graph = testGraph.hugeGraph();

        // Clear schema
        SchemaTransaction schema = graph.schemaTransaction();

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

        graph.tx().commit();
    }

    @Override
    public void clear(Graph graph, Configuration configuration)
           throws Exception {
        if (graph != null && graph.tx().isOpen()) {
            HugeGraph hugeGraph = ((TestGraph) graph).hugeGraph();
            hugeGraph.tx().commit();
            hugeGraph.clearBackend();
            hugeGraph.close();
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void loadGraphData(final Graph graph,
                              final LoadGraphWith loadGraphWith,
                              final Class testClass,
                              final String testName) {
        if (loadGraphWith == null) {
            super.loadGraphData(graph, loadGraphWith, testClass, testName);
            return;
        }

        TestGraph testGraph = (TestGraph) graph;
        // Clear basic schema initiated in openTestGraph
        this.clearSchemaData(testGraph);

        testGraph.loadedGraph(true);
        switch (loadGraphWith.value()) {
            case GRATEFUL:
                testGraph.initGratefulSchema();
                break;
            case MODERN:
                testGraph.initModernSchema();
                break;
            case CLASSIC:
                testGraph.initClassicSchema();
                break;
            default:
                throw new AssertionError(String.format(
                          "Only support GRATEFUL, MODERN and CLASSIC " +
                          "for @LoadGraphWith(), but '%s' is used ",
                          loadGraphWith.value()));
        }

        testGraph.tx().commit();
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @Override
    public GraphTraversalSource traversal(Graph graph) {
        return ((TestGraph) graph).hugeGraph().traversal();
    }
}
