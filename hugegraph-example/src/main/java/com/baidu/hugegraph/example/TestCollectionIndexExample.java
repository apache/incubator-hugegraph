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

package com.baidu.hugegraph.example;

import java.util.Arrays;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.traversal.optimize.ConditionP;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestCollectionIndexExample {

    private static final Logger LOG = Log.logger(Example3.class);

    public static void main(String[] args) throws Exception {
        LOG.info("TestIndexExample start!");

        HugeGraph graph = ExampleUtil.loadGraph();

        initData(graph);
        queryTest(graph);

        graph.close();

        HugeFactory.shutdown(30L);
    }

    public static void initData(final HugeGraph graph) {
        SchemaManager schema = graph.schema();

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("weight").asDouble().create();
        schema.propertyKey("tags").asText().valueSet().create();
        schema.propertyKey("category").asText().valueSet().create();
        schema.propertyKey("country").asText().create();

        schema.vertexLabel("soft").properties("name", "tags", "country", "category")
              .primaryKeys("name").create();

        graph.addVertex(T.label, "soft", "name", "hugegraph",
                        "country", "china",
                        "category", Arrays.asList("graphdb", "db"),
                        "tags", Arrays.asList("graphdb", "gremlin"));

        graph.addVertex(T.label, "soft", "name", "neo4j",
                        "country", "usa",
                        "category", Arrays.asList("graphdb", "db"),
                        "tags", Arrays.asList("graphdb", "cypher"));

        graph.addVertex(T.label, "soft", "name", "jenagraph",
                        "country", "usa",
                        "category", Arrays.asList("graphdb", "javadb"),
                        "tags", Arrays.asList("graphdb", "gremlin"));

        graph.tx().commit();


        schema.indexLabel("softByTag").onV("soft").secondary()
              .by("tags").create();

        schema.indexLabel("softByCategory").onV("soft").search()
              .by("category").create();

    }

    public static void queryTest(final HugeGraph graph) {
        List<Vertex> vertices;
        vertices = graph.traversal().V().has("soft", "category",
                                             ConditionP.textContains("graphdb" +
                                                                     " db")).toList();
        Assert.assertEquals(3, vertices.size());

        // by single item
        vertices = graph.traversal().V().has("soft", "tags", "gremlin").toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V().has("soft", "tags", ImmutableList.of(
                "graphdb", "graphdb")).toList();
        Assert.assertEquals(3, vertices.size());

        // by contains
        vertices = graph.traversal().V().has("soft", "tags", ConditionP.contains("gremlin")).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V().has("soft", "tags", ImmutableSet.of(
                "graphdb")).toList();
        Assert.assertEquals(3, vertices.size());

        // collection search
        vertices = graph.traversal().V().has("soft", "tags", ImmutableSet.of(
                "gremlin",
                "graphdb")).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V().has("soft", "tags", ImmutableSet.of(
                "cypher",
                "graphdb")).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V().has("soft", "tags", ImmutableSet.of(
                "gremlin",
                "graphdb")).toList();
        Assert.assertEquals(2, vertices.size());

        Vertex vertex = vertices.get(0);

        vertex.property("tags",  ImmutableSet.of("new_tag"));

        graph.tx().commit();

        vertices = graph.traversal().V().has("soft", "tags", ImmutableSet.of(
                "cypher",
                "graphdb")).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V().has("soft", "tags", ImmutableSet.of(
                "new_tag")).toList();
        Assert.assertEquals(1, vertices.size());

        graph.tx().close();

    }


}
