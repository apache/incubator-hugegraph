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

package com.baidu.hugegraph.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Utils;

import jersey.repackaged.com.google.common.collect.ImmutableList;
import jersey.repackaged.com.google.common.collect.ImmutableMap;

public class MultiGraphsTest {

    private static final Map<String, String> BACKENDS =
            ImmutableMap.<String, String>builder()
            .put("memory", "text")
            .put("rocksdb", "binary")
            .put("cassandra", "cassandra")
            .put("hbase", "hbase")
            .put("mysql", "mysql")
            .put("postgresql", "postgresql")
            .build();

    @Test
    public void testCreateMultiGraphs() {
        List<HugeGraph> graphs = openGraphs("g1", "g2", "g3", "123",
                                            " g", "g 1", " .", ". .",
                                            "@$%^&*()_+`-={}|[]\"<?;'~,./\\",
                                            "azAZ0123456789", " ~", "g~", "g'");
        destoryGraphs(graphs);
    }

    @Test
    public void testCreateGraphsWithInvalidNames() {
        Assert.assertThrows(RuntimeException.class,
                            () -> openGraphs(""));
    }

    @Test
    public void testCreateGraphsWithSameName() {
        List<HugeGraph> graphs = openGraphs("graph", "graph");
        HugeGraph g1 = graphs.get(0);
        HugeGraph g2 = graphs.get(1);

        g1.initBackend();
        g2.initBackend();

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> g2.vertexLabel("node"));
        g1.schema().vertexLabel("node").useCustomizeNumberId()
          .ifNotExist().create();
        g2.vertexLabel("node");

        g1.addVertex(T.label, "node", T.id, 1);
        Iterator<Vertex> vertices = g2.vertices(1);
        Assert.assertTrue(vertices.hasNext());
        Vertex vertex = vertices.next();
        Assert.assertFalse(vertices.hasNext());
        Assert.assertEquals(IdGenerator.of(1), vertex.id());

        destoryGraphs(ImmutableList.of(g1));
    }

    @Test
    public void testCreateGraphWithSameNameDifferentBackends() {
        openGraphWithBackend("graph", "memory");
        Assert.assertThrows(RuntimeException.class,
                            () -> openGraphWithBackend("graph", "rocksdb"));
    }

    @Test
    public void testCreateGraphsWithDifferentNameDifferentBackends() {
        HugeGraph g1 = openGraphWithBackend("g1", "memory");
        HugeGraph g2 = openGraphWithBackend("g2", "rocksdb");
        HugeGraph graph = openGraphs("graph").get(0);
        destoryGraphs(ImmutableList.of(g1, g2, graph));
    }

    public static List<HugeGraph> openGraphs(String... graphNames) {
        List<HugeGraph> graphs = new ArrayList<>(graphNames.length);
        PropertiesConfiguration conf = Utils.getConf();
        Configuration config = new BaseConfiguration();
        for (Iterator<String> keys = conf.getKeys(); keys.hasNext();) {
            String key = keys.next();
            config.setProperty(key, conf.getProperty(key));
        }
        ((BaseConfiguration) config).setDelimiterParsingDisabled(true);
        for (String graphName : graphNames) {
            config.setProperty(CoreOptions.STORE.name(), graphName);
            graphs.add((HugeGraph) GraphFactory.open(config));
        }
        return graphs;
    }

    public static void destoryGraphs(List<HugeGraph> graphs) {
        for (HugeGraph graph : graphs) {
            graph.close();
        }
    }

    public static HugeGraph openGraphWithBackend(String graph, String backend) {
        PropertiesConfiguration conf = Utils.getConf();
        Configuration config = new BaseConfiguration();
        for (Iterator<String> keys = conf.getKeys(); keys.hasNext();) {
            String key = keys.next();
            config.setProperty(key, conf.getProperty(key));
        }
        ((BaseConfiguration) config).setDelimiterParsingDisabled(true);
        config.setProperty(CoreOptions.STORE.name(), graph);
        config.setProperty(CoreOptions.BACKEND.name(), backend);
        config.setProperty(CoreOptions.SERIALIZER.name(),
                           BACKENDS.get(backend));
        return ((HugeGraph) GraphFactory.open(config));
    }
}
