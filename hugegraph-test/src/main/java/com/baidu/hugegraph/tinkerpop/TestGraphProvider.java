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
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.Assert;
import org.junit.Assume;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableSet;

public class TestGraphProvider extends AbstractGraphProvider {

    private static final Logger LOG = Log.logger(TestGraphProvider.class);

    private static final String CONF_PATH = "hugegraph.properties";
    private static final String FILTER = "test.tinkerpop.filter";
    private static final String DEFAULT_FILTER = "methods.filter";

    private static final String TEST_CLASS = "testClass";
    private static final String TEST_METHOD = "testMethod";

    private static final String LOAD_GRAPH = "loadGraph";
    private static final String STANDARD = "standard";
    private static final String REGULAR_LOAD = "regularLoad";

    private static final String GREMLIN_GRAPH_KEY = "gremlin.graph";
    private static final String GREMLIN_GRAPH_VALUE =
            "com.baidu.hugegraph.tinkerpop.TestGraphFactory";

    private static final String AKEY_CLASS_PREFIX =
            "org.apache.tinkerpop.gremlin.structure." +
            "PropertyTest.PropertyFeatureSupportTest";
    private static final String IO_CLASS_PREFIX =
            "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest";
    private static final String IO_TEST_PREFIX =
            "org.apache.tinkerpop.gremlin.structure.io.IoTest";

    private Map<String, String> blackMethods = new HashMap<>();
    private Map<String, TestGraph> graphs = new HashMap<>();
    private final String suite;

    public TestGraphProvider(String suite) throws IOException {
        super();
        this.initBlackList();
        this.suite = suite;
    }

    private void initBlackList() throws IOException {
        String filter = (String) getConf().getProperty(FILTER);
        if (filter == null || filter.isEmpty()) {
            filter = DEFAULT_FILTER;
        }

        URL blackList = TestGraphProvider.class.getClassLoader()
                                               .getResource(filter);
        E.checkArgument(blackList != null,
                        "Can't find tests filter '%s' in resource directory",
                        filter);
        File file = new File(blackList.getPath());
        E.checkArgument(
                file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable filter file rather than: %s",
                file.toString());
        try (FileReader fr = new FileReader(file);
             BufferedReader reader = new BufferedReader(fr)) {
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
                        "Reason why ignore in methods.filter can't be empty",
                        parts[1] != null && !parts[1].trim().isEmpty());
                this.blackMethods.putIfAbsent(parts[0], parts[1]);
            }
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

    public PropertiesConfiguration getConf() {
        String confFile = TestGraphProvider.class.getClassLoader()
                                           .getResource(CONF_PATH).getPath();
        File file = new File(confFile);
        E.checkArgument(
                file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable config file rather than: %s",
                file.toString());

        PropertiesConfiguration config;
        try {
            config = new PropertiesConfiguration(file);
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load config file: %s",
                                    e, confFile);
        }
        return config;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(
                               String graphName,
                               Class<?> testClass, String testMethod,
                               LoadGraphWith.GraphData graphData) {
        // Check if test in blackList
        String testFullName = testClass.getCanonicalName() + "." + testMethod;
        int index = testFullName.indexOf('@') == -1 ?
                    testFullName.length() : testFullName.indexOf('@');

        testFullName = testFullName.substring(0, index);
        Assume.assumeFalse(
               String.format("Test %s will be ignored with reason: %s",
                             testFullName, this.blackMethods.get(testFullName)),
               this.blackMethods.containsKey(testFullName));

        LOG.debug("Full name of test is: {}", testFullName);
        LOG.debug("Prefix of test is: {}", testFullName.substring(0, index));
        HashMap<String, Object> confMap = new HashMap<>();
        PropertiesConfiguration config = getConf();
        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            confMap.put(key, config.getProperty(key));
        }
        String storePrefix = (String) config.getProperty(
                                      CoreOptions.STORE.name());
        confMap.put(CoreOptions.STORE.name(),
                    storePrefix + "_" + this.suite + "_" + graphName);
        confMap.put(GREMLIN_GRAPH_KEY, GREMLIN_GRAPH_VALUE);
        confMap.put(TEST_CLASS, testClass);
        confMap.put(TEST_METHOD, testMethod);
        confMap.put(LOAD_GRAPH, graphData);

        return confMap;
    }

    private static String getAKeyType(Class<?> clazz, String method) {
        if (clazz.getCanonicalName().startsWith(AKEY_CLASS_PREFIX)) {
            return method.substring(method.indexOf('[') + 9,
                                    method.indexOf('(') - 6);
        }
        return null;
    }

    private static String getIoType(Class<?> clazz, String method) {
        if (clazz.getCanonicalName().startsWith(IO_CLASS_PREFIX)) {
            return method.substring(method.indexOf('[') + 1,
                                    method.indexOf(']'));
        }
        return null;
    }

    private static boolean ioTest(Class<?> clazz) {
        return clazz.getCanonicalName().startsWith(IO_TEST_PREFIX);
    }

    @Override
    public Graph openTestGraph(final Configuration config) {
        String graphName = config.getString(CoreOptions.STORE.name());
        TestGraph testGraph = this.graphs.get(graphName);
        if (testGraph == null) {
            this.graphs.putIfAbsent(graphName, this.newTestGraph(config));
        } else if (testGraph.closed()) {
            this.graphs.put(graphName, this.newTestGraph(config));
        }
        testGraph = this.graphs.get(graphName);

        // Ensure tx clean
        testGraph.tx().rollback();

        // Define property key 'aKey' based on specified type in test name
        Class<?> testClass = (Class<?>) config.getProperty(TEST_CLASS);
        String testMethod = config.getString(TEST_METHOD);
        String aKeyType = getAKeyType(testClass, testMethod);
        if (aKeyType != null) {
            testGraph.initPropertyKey("aKey", aKeyType);
        }

        // Basic schema is initiated by default once a graph is open
        testGraph.initBasicSchema(IdStrategy.AUTOMATIC, TestGraph.DEFAULT_VL);
        testGraph.tx().commit();

        testGraph.isLastIdCustomized(false);
        testGraph.loadedGraph(getIoType(testClass, testMethod));
        testGraph.autoPerson(false);
        testGraph.ioTest(ioTest(testClass));

        Object loadGraph = config.getProperty(LOAD_GRAPH);
        if (loadGraph != null && !graphName.endsWith(STANDARD)) {
            this.loadGraphData(testGraph, (LoadGraphWith.GraphData) loadGraph);
        }

        LOG.debug("Open graph '{}' for test '{}'", graphName, testMethod);
        return testGraph;
    }

    private TestGraph newTestGraph(final Configuration config) {
        TestGraph testGraph = ((TestGraph) super.openTestGraph(config));
        testGraph.initBackend();
        return testGraph;
    }

    @Override
    public void clear(Graph graph, Configuration config) throws Exception {
        TestGraph testGraph = (TestGraph) graph;
        if (testGraph == null || !testGraph.initedBackend()) {
            return;
        }
        String graphName = config.getString(CoreOptions.STORE.name());
        if (testGraph.closed()) {
            this.graphs.remove(graphName);
            return;
        }

        // Reset consumers
        graph.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.AUTO);
        graph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);

        // Ensure tx clean
        graph.tx().rollback();

        // Clear all data
        Class<?> testClass = (Class<?>) config.getProperty(TEST_CLASS);
        testGraph.clearAll(testClass.getCanonicalName());

        LOG.debug("Clear graph '{}'", graphName);
    }

    public void clearBackends() {
        for (TestGraph graph : this.graphs.values()) {
            graph.clearBackend();
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

        loadGraphData(graph, loadGraphWith.value());

        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }

    public void loadGraphData(final Graph graph,
                              final LoadGraphWith.GraphData loadGraphWith) {
        TestGraph testGraph = (TestGraph) graph;

        // Clear basic schema initiated in openTestGraph
        testGraph.clearAll("");

        if (testGraph.loadedGraph() == null) {
            testGraph.loadedGraph(REGULAR_LOAD);
        }

        switch (loadGraphWith) {
            case GRATEFUL:
                testGraph.initGratefulSchema();
                break;
            case MODERN:
                testGraph.initModernSchema();
                break;
            case CLASSIC:
                testGraph.initClassicSchema();
                break;
            case CREW:
                break;
            default:
                throw new AssertionError(String.format(
                          "Only support GRATEFUL, MODERN and CLASSIC " +
                          "for @LoadGraphWith(), but '%s' is used ",
                          loadGraphWith));
        }
        testGraph.tx().commit();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @SuppressWarnings("unchecked")
    @Override
    public GraphTraversalSource traversal(Graph graph) {
        HugeGraph hugegraph = ((TestGraph) graph).hugeGraph();
        return hugegraph.traversal()
                        .withoutStrategies(LazyBarrierStrategy.class);
    }

    @Override
    public String convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }
}
