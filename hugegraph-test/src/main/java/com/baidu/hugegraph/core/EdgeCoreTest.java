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

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.exception.LimitExceedException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.FakeObjects.FakeEdge;
import com.baidu.hugegraph.testutil.Utils;
import com.baidu.hugegraph.traversal.optimize.Text;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class EdgeCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  propertyKey  ================");

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("dynamic").asBoolean().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("timestamp").asLong().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueSet().create();
        schema.propertyKey("contribution").asText().create();
        schema.propertyKey("score").asInt().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("amount").asFloat().create();
        schema.propertyKey("message").asText().create();
        schema.propertyKey("place").asText().create();
        schema.propertyKey("tool").asText().create();
        schema.propertyKey("reason").asText().create();
        schema.propertyKey("hurt").asBoolean().create();
        schema.propertyKey("arrested").asBoolean().create();
        schema.propertyKey("date").asDate().create();

        LOG.debug("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("language")
              .properties("name", "dynamic")
              .primaryKeys("name")
              .nullableKeys("dynamic")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();

        LOG.debug("===============  edgeLabel  ================");

        schema.edgeLabel("transfer")
              .properties("id", "amount", "timestamp", "message")
              .nullableKeys("message")
              .multiTimes().sortKeys("id")
              .link("person", "person")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("authored").singleTime()
              .properties("contribution", "comment", "score")
              .nullableKeys("score", "contribution", "comment")
              .link("author", "book")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("write").properties("time")
              .multiTimes().sortKeys("time")
              .link("author", "book")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("look").properties("time", "score")
              .nullableKeys("score")
              .multiTimes().sortKeys("time")
              .link("person", "book")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("know").singleTime()
              .link("author", "author")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("followedBy").singleTime()
              .link("author", "person")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("friend").singleTime()
              .link("person", "person")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("follow").singleTime()
              .link("person", "author")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("created").singleTime()
              .link("author", "language")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("strike").link("person", "person")
              .properties("id", "timestamp", "place", "tool", "reason",
                          "hurt", "arrested")
              .multiTimes().sortKeys("id")
              .nullableKeys("tool", "reason", "hurt")
              .enableLabelIndex(false)
              .ifNotExist().create();
    }

    protected void initTransferIndex() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  transfer index  ================");

        schema.indexLabel("transferByTimestamp").onE("transfer").range()
              .by("timestamp").create();
    }

    protected void initStrikeIndex() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  strike index  ================");

        schema.indexLabel("strikeByTimestamp").onE("strike").range()
              .by("timestamp").create();
        schema.indexLabel("strikeByPlace").onE("strike").secondary()
              .by("tool").create();
        schema.indexLabel("strikeByPlaceToolReason").onE("strike").secondary()
              .by("place", "tool", "reason").create();
    }

    @Test
    public void testAddEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido =  graph.addVertex(T.label, "author", "id", 2,
                                        "name", "Guido van Rossum", "age", 61,
                                        "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(5, edges.size());
        assertContains(edges, "created", james, java);
        assertContains(edges, "created", guido, python);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3);
    }

    @Test
    public void testAddEdgeWithOverrideEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido =  graph.addVertex(T.label, "author", "id", 2,
                                        "name", "Guido van Rossum", "age", 61,
                                        "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        // Group 1
        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("created", java);
        guido.addEdge("created", python);

        // Group 2
        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3, "score", 4);

        james.addEdge("authored", java1);
        james.addEdge("authored", java3, "score", 5);

        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(5, edges.size());
        assertContains(edges, "created", james, java);
        assertContains(edges, "created", guido, python);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3, "score", 5);
    }

    @Test
    public void testAddEdgeWithProp() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                      "name", "James Gosling", "age", 62,
                                      "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("write", book, "time", "2017-4-28");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "write", james, book,
                       "time", "2017-4-28");
    }

    @Test
    public void testAddEdgeWithProps() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                      "contribution", "1990-1-1", "score", 5);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book,
                       "contribution", "1990-1-1", "score", 5);
    }

    @Test
    public void testAddEdgeWithPropSet() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                      "comment", "good book!",
                      "comment", "good book too!");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book);
        Edge edge = edges.get(0);
        Object comments = edge.property("comment").value();
        Assert.assertEquals(ImmutableSet.of("good book!", "good book too!"),
                            comments);
    }

    @Test
    public void testAddEdgeWithPropSetAndOverridProp() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                      "comment", "good book!",
                      "comment", "good book!",
                      "comment", "good book too!");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book);
        Edge edge = edges.get(0);
        Object comments = edge.property("comment").value();
        Assert.assertEquals(ImmutableSet.of("good book!", "good book too!"),
                            comments);
    }

    @Test
    public void testAddEdgeToSameVerticesWithMultiTimes() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("write", book, "time", "2017-4-28");
        james.addEdge("write", book, "time", "2017-5-21");
        james.addEdge("write", book, "time", "2017-5-25");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(3, edges.size());
        assertContains(edges, "write", james, book,
                       "time", "2017-4-28");
        assertContains(edges, "write", james, book,
                       "time", "2017-5-21");
        assertContains(edges, "write", james, book,
                       "time", "2017-5-25");
    }

    @Test
    public void testAddEdgeToSameVerticesWithMultiTimesAndOverrideEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("write", book, "time", "2017-4-28");
        james.addEdge("write", book, "time", "2017-5-21");
        james.addEdge("write", book, "time", "2017-5-25");

        james.addEdge("write", book, "time", "2017-4-28");
        james.addEdge("write", book, "time", "2017-5-21");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(3, edges.size());
        assertContains(edges, "write", james, book,
                       "time", "2017-4-28");
        assertContains(edges, "write", james, book,
                       "time", "2017-5-21");
        assertContains(edges, "write", james, book,
                       "time", "2017-5-25");
    }

    @Test
    public void testAddEdgeWithNotExistsEdgeLabel() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("label-not-exists", book, "time", "2017-4-28");
        });
    }

    @Test
    public void testAddEdgeWithoutSortkey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("write", book);
        });
    }

    @Test
    public void testAddEdgeWithLargeSortkey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            final int LEN = BytesBuffer.BIG_ID_MAX_LEN;
            String largeTime = new String(new byte[LEN]) + "{large-time}";
            james.addEdge("write", book, "time", largeTime);
            graph.tx().commit();
        });
    }

    @Test
    public void testAddEdgeWithNotExistsPropKey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "propkey-not-exists", "value");
        });
    }

    @Test
    public void testAddEdgeWithNotExistsEdgePropKey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "age", 18);
        });
    }

    @Test
    public void testAddEdgeWithNullableKeysAbsent() {
        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");

        Edge edge = baby.addEdge("look", java, "time", "2017-09-09");
        Assert.assertEquals("2017-09-09", edge.value("time"));
    }

    @Test
    public void testAddEdgeWithNonNullKeysAbsent() {
        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'time'
            baby.addEdge("look", java, "score", 99);
        });
    }

    @Test
    public void testAddEdgeWithInvalidPropValueType() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book, "score", 5);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "score", 5.1);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "score", "five");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "score", "5");
        });
    }

    @Test
    public void testQueryAllEdges() {
        HugeGraph graph = graph();
        init18Edges();

        // All edges
        List<Edge> edges = graph.traversal().E().toList();

        Assert.assertEquals(18, edges.size());

        Vertex james = vertex("author", "id", 1);
        Vertex guido = vertex("author", "id", 2);

        Vertex java = vertex("language", "name", "java");
        Vertex python = vertex("language", "name", "python");

        Vertex java1 = vertex("book", "name", "java-1");
        Vertex java2 = vertex("book", "name", "java-2");
        Vertex java3 = vertex("book", "name", "java-3");

        assertContains(edges, "created", james, java);
        assertContains(edges, "created", guido, python);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3);
    }

    @Test
    public void testQueryEdgesWithOrderBy() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().V()
                           .hasLabel("person").has("name", "Louise")
                           .outE("look").order().by("time")
                           .toList();
        Assert.assertEquals(4, edges.size());
        Assert.assertEquals("2017-5-1", edges.get(0).value("time"));
        Assert.assertEquals("2017-5-1", edges.get(1).value("time"));
        Assert.assertEquals("2017-5-27", edges.get(2).value("time"));
        Assert.assertEquals("2017-5-27", edges.get(3).value("time"));
    }

    @Test
    public void testQueryEdgesWithOrderByAndIncidentToAdjacentStrategy() {
        HugeGraph graph = graph();
        init18Edges();

        List<Vertex> vertices = graph.traversal().V()
                                .hasLabel("person").has("name", "Louise")
                                .outE("look").inV().toList();
        /*
         * This should be 4 vertices, but the gremlin:
         * ".outE("look").inV()" will be replaced with ".out("look")",
         * then the duplicate edges would be removed by core.
         * For more details see IncidentToAdjacentStrategy.
         */
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V()
                   .hasLabel("person").has("name", "Louise")
                   .outE("look").order().by("time").inV().toList();
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testQueryAllWithLimit() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E().limit(10).toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().E().limit(12).limit(10).toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().E().limit(10).limit(12).toList();
        Assert.assertEquals(10, edges.size());
    }

    @Test
    public void testQueryAllWithLimitByQueryEdges() {
        HugeGraph graph = graph();
        init18Edges();

        Query query = new Query(HugeType.EDGE);
        query.limit(1);
        Iterator<Edge> itor = graph.graphTransaction().queryEdges(query);
        List<Edge> edges = IteratorUtils.list(itor);
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryAllWithLimit0() {
        HugeGraph graph = graph();
        init18Edges();

        // Query all with limit 0
        List<Edge> edges = graph.traversal().E().limit(0).toList();

        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryAllWithNoLimit() {
        HugeGraph graph = graph();
        init18Edges();

        // Query all with limit -1 (mean no-limit)
        List<Edge> edges = graph.traversal().E().limit(-1).toList();
        Assert.assertEquals(18, edges.size());
    }

    @Test
    public void testQueryAllWithIllegalLimit() {
        HugeGraph graph = graph();
        init18Edges();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().limit(-2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().limit(-20).toList();
        });
    }

    @Test
    public void testQueryAllWithOffset() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E().range(8, 100).toList();
        Assert.assertEquals(10, edges.size());

        List<Edge> edges2 = graph.traversal().E().range(8, -1).toList();
        Assert.assertEquals(edges, edges2);
    }

    @Test
    public void testQueryAllWithOffsetAndLimit() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E().range(8, 9).toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().E().range(0, 4).toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().E().range(-2, 4).toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().E().range(18, -1).toList();
        Assert.assertEquals(0, edges.size());

        edges = graph.traversal().E().range(0, -1).toList();
        Assert.assertEquals(18, edges.size());

        edges = graph.traversal().E().range(-2, -1).toList();
        Assert.assertEquals(18, edges.size());
    }

    @Test
    public void testQueryAllWithOffsetAndLimitWithMultiTimes() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E()
                                .range(1, 6)
                                .range(4, 8)
                                .toList();
        // [4, 6)
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E()
                                 .range(1, -1)
                                 .range(6, 8)
                                 .toList();
        // [6, 8)
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E()
                                 .range(1, 6)
                                 .range(6, 8)
                                 .toList();
        // [6, 6)
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryAllWithIllegalOffsetOrLimit() {
        HugeGraph graph = graph();
        init18Edges();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().range(8, 7).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().range(-1, -2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().range(0, -2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().range(-4, -2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // [7, 6)
            graph.traversal().E().range(1, 6).range(7, 8).toList();
        });
    }

    @Test
    public void testQueryEdgesWithLimitOnMultiLevel() {
        HugeGraph graph = graph();
        init18Edges();
        Vertex james = vertex("author", "id", 1);

        List<Edge> edges = graph.traversal().V()
                           .hasLabel("person").has("name", "Louise")
                           .outE("look").inV()
                           .inE("authored")
                           .toList();
        Assert.assertEquals(3, edges.size());
        Assert.assertEquals(james, edges.get(0).outVertex());
        Assert.assertEquals(james, edges.get(1).outVertex());
        Assert.assertEquals(james, edges.get(2).outVertex());

        edges = graph.traversal().V()
                .hasLabel("person").has("name", "Louise")
                .outE("look").limit(2).inV()
                .inE("authored").limit(3)
                .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V()
                .hasLabel("person").has("name", "Louise")
                .outE("look").limit(2).inV()
                .inE("authored").limit(2)
                .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V()
                .hasLabel("person").has("name", "Louise")
                .outE("look").limit(2).inV()
                .inE("authored").limit(1)
                .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V()
                .hasLabel("person").has("name", "Louise")
                .outE("look").limit(1).inV()
                .inE("authored").limit(2)
                .toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryEdgesWithLimitAndOrderBy() {
        Assume.assumeTrue("Not support order by",
                          storeFeatures().supportsQueryWithOrderBy());
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().V()
                           .hasLabel("person").has("name", "Louise")
                           .outE("look").order().by("time")
                           .limit(2).toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals("2017-5-1", edges.get(0).value("time"));
        Assert.assertEquals("2017-5-1", edges.get(1).value("time"));

        edges = graph.traversal().V()
                .hasLabel("person").has("name", "Louise")
                .outE("look").order().by("time", Order.decr)
                .limit(2).toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals("2017-5-27", edges.get(0).value("time"));
        Assert.assertEquals("2017-5-27", edges.get(1).value("time"));

        edges = graph.traversal().V()
                .hasLabel("person").has("name", "Louise")
                .outE("look").limit(2)
                .order().by("time", Order.decr)
                .toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals("2017-5-1", edges.get(0).value("time"));
        Assert.assertEquals("2017-5-1", edges.get(1).value("time"));
    }

    @Test
    public void testQueryEdgesById() {
        HugeGraph graph = graph();
        init18Edges();

        Object id = graph.traversal().E().toList().get(0).id();
        List<Edge> edges = graph.traversal().E(id).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryEdgesByIdNotFound() {
        HugeGraph graph = graph();
        init18Edges();

        String id = graph.traversal().E().toList().get(0).id() + "-not-exist";
        Assert.assertTrue(graph.traversal().E(id).toList().isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            graph.traversal().E(id).next();
        });
    }

    @Test
    public void testQueryEdgesByInvalidId() {
        HugeGraph graph = graph();
        init18Edges();

        String id = "invalid-id";
        Assert.assertThrows(NotFoundException.class, () -> {
            graph.traversal().E(id).toList();
        });
    }

    @Test
    public void testQueryEdgesByLabel() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E().hasLabel("created").toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().hasLabel("authored").toList();
        Assert.assertEquals(3, edges.size());

        edges = graph.traversal().E().hasLabel("look").toList();
        Assert.assertEquals(7, edges.size());

        edges = graph.traversal().E().hasLabel("friend").toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().E().hasLabel("follow").toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().E().hasLabel("know").toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryEdgesByDirection() {
        HugeGraph graph = graph();
        init18Edges();

        // Query vertex by condition (filter by Direction)
        ConditionQuery q = new ConditionQuery(HugeType.EDGE);
        q.eq(HugeKeys.DIRECTION, Direction.OUT);

        Assert.assertThrows(BackendException.class, () -> {
            graph.edges(q);
        });
    }

    @Test
    public void testQueryBothEdgesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        // Query edges of a vertex
        Vertex james = vertex("author", "id", 1);
        List<Edge> edges = graph.traversal().V(james.id()).bothE().toList();
        Assert.assertEquals(6, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.BOTH));
        Assert.assertEquals(6, edges.size());
    }

    @Test
    public void testQueryBothVerticesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex jeff = vertex("person", "name", "Jeff");

        List<Vertex> vertices = graph.traversal().V(jeff.id())
                                .both("friend").toList();
        Assert.assertEquals(2, vertices.size());

        vertices = ImmutableList.copyOf(
                   jeff.vertices(Direction.BOTH, "friend"));
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testQueryOutEdgesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        // Query OUT edges of a vertex
        Vertex james = vertex("author", "id", 1);
        List<Edge> edges = graph.traversal().V(james.id()).outE().toList();

        Assert.assertEquals(4, edges.size());
    }

    @Test
    public void testQueryOutVerticesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex james = vertex("author", "id", 1);
        List<Vertex> vertices = graph.traversal().V(james.id()).out().toList();

        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testQueryOutEdgesOfVertexBySortkey() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex louise = vertex("person", "name", "Louise");

        List<Edge> edges = graph.traversal().V(louise.id()).outE("look")
                                .has("time", "2017-5-1").toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(louise.id())
                     .outE("look").has("time", "2017-5-27")
                     .toList();
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryOutEdgesOfVertexBySortkeyAndProps() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex louise = vertex("person", "name", "Louise");

        List<Edge> edges = graph.traversal().V(louise.id()).outE("look")
                                .has("time", "2017-5-1").has("score", 3)
                                .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(louise.id()).outE("look")
                     .has("time", "2017-5-27").has("score", 3)
                     .toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryOutEdgesOfVertexByRangeFilter() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authoredByScore").onE("authored")
             .range().by("score").create();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book1 = graph.addVertex(T.label, "book", "name", "Test-Book-1");
        Vertex book2 = graph.addVertex(T.label, "book", "name", "Test-Book-2");
        Vertex book3 = graph.addVertex(T.label, "book", "name", "Test-Book-3");

        james.addEdge("authored", book1,
                      "contribution", "1991 3 1", "score", 5);
        james.addEdge("authored", book2,
                      "contribution", "1992 2 2", "score", 4);
        james.addEdge("authored", book3,
                      "contribution", "1993 3 2", "score", 3);

        graph.tx().commit();

        // Won't query by search index, just filter by property after outE()
        List<Edge> edges = graph.traversal().V(james).outE("authored")
                                .has("score", P.gte(4)).toList();
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexWithResultN() {
        HugeGraph graph = graph();
        init18Edges();

        // Query IN edges of a vertex
        Vertex james = vertex("author", "id", 1);
        List<Edge> edges = graph.traversal().V(james.id()).inE().toList();

        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexWithResult1() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java = vertex("language", "name", "java");
        List<Edge> edges = graph.traversal().V(java.id()).inE().toList();

        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexWithResult0() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex guido = vertex("author", "id", 2);
        List<Edge> edges = graph.traversal().V(guido.id()).inE().toList();

        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexByLabel() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        List<Edge> edges = graph.traversal().V(java3.id()).inE().toList();
        Assert.assertEquals(5, edges.size());

        edges = graph.traversal().V(java3.id()).inE("look").toList();
        Assert.assertEquals(4, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexByLabelAndFilter() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        List<Edge> edges = graph.traversal().V(java3.id())
                           .inE().has("score", 3).toList();
        Assert.assertEquals(3, edges.size());

        edges = graph.traversal().V(java3.id())
                .inE("look").has("score", 3).toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(java3.id())
                .inE("look").has("score", 4).toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(java3.id())
                .inE("look").has("score", 0).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexBySortkey() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        List<Edge> edges = graph.traversal().V(java3.id())
                           .inE("look").toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().V(java3.id())
                .inE("look").has("time", "2017-5-27").toList();
        Assert.assertEquals(3, edges.size());
    }

    @Test
    public void testQueryInVerticesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        List<Vertex> vertices = graph.traversal().V(java3.id())
                                .in("look").toList();
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testQueryInVerticesOfVertexAndFilter() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        // NOTE: the has() just filter by vertex props
        List<Vertex> vertices = graph.traversal().V(java3.id())
                                .in("look").has("age", P.gt(22))
                                .toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testQueryByLongPropOfOverrideEdge() {
        HugeGraph graph = graph();
        initTransferIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1, "timestamp", current,
                       "place", "park", "tool", "shovel", "reason", "jeer",
                       "arrested", false);
        louise.addEdge("strike", sean, "id", 1, "timestamp", current + 1,
                       "place", "park", "tool", "shovel", "reason", "jeer",
                       "arrested", false);
        List<Edge> edges = graph.traversal().E().has("timestamp", current)
                                .toList();
        Assert.assertEquals(0, edges.size());
        edges = graph.traversal().E().has("timestamp", current + 1).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryByStringPropOfOverrideEdge() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1, "timestamp", current,
                       "place", "park", "tool", "shovel", "reason", "jeer",
                       "arrested", false);
        louise.addEdge("strike", sean, "id", 1, "timestamp", current,
                       "place", "street", "tool", "shovel", "reason", "jeer",
                       "arrested", false);
        List<Edge> edges = graph.traversal().E().has("place", "park")
                                .toList();
        Assert.assertEquals(0, edges.size());
        edges = graph.traversal().E().has("place", "street").toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryByDateProperty() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.edgeLabel("buy")
              .properties("place", "date")
              .link("person", "book")
              .create();
        schema.indexLabel("buyByDate").onE("buy").by("date").range().create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                "city", "Beijing", "age", 23);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        Date[] dates = new Date[]{
                Utils.date("2012-01-01 12:30:00.100"),
                Utils.date("2013-01-01 12:30:00.100"),
                Utils.date("2014-01-01 12:30:00.100")
        };

        louise.addEdge("buy", java1, "place", "haidian", "date", dates[0]);
        jeff.addEdge("buy", java2, "place", "chaoyang", "date", dates[1]);
        sean.addEdge("buy", java3, "place", "chaoyang", "date", dates[2]);

        List<Edge> edges = graph.traversal().E().hasLabel("buy")
                                .has("date", dates[0])
                                .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(dates[0], edges.get(0).value("date"));

        edges = graph.traversal().E().hasLabel("buy")
                     .has("date", P.gt(dates[0]))
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().hasLabel("buy")
                     .has("date", P.between(dates[1], dates[2]))
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(dates[1], edges.get(0).value("date"));
    }

    public void testQueryByOutEWithDateProperty() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.edgeLabel("buy")
              .properties("place", "date")
              .link("person", "book")
              .create();
        schema.indexLabel("buyByDate").onE("buy").by("date").range().create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        String[] dates = new String[]{
                "2012-01-01 12:30:00.100",
                "2013-01-01 12:30:00.100",
                "2014-01-01 12:30:00.100"
        };

        louise.addEdge("buy", java1, "place", "haidian", "date", dates[0]);
        jeff.addEdge("buy", java2, "place", "chaoyang", "date", dates[1]);
        sean.addEdge("buy", java3, "place", "chaoyang", "date", dates[2]);

        List<Edge> edges = graph.traversal().V().outE()
                                .has("date", P.between(dates[0], dates[2]))
                                .toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals(Utils.date(dates[0]), edges.get(0).value("date"));
        Assert.assertEquals(Utils.date(dates[1]), edges.get(1).value("date"));
    }

    @Test
    public void testQueryByTextContainsProperty() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authoredByContribution").onE("authored")
             .search().by("contribution").create();
        graph.schema().indexLabel("authoredByScore").onE("authored")
             .range().by("score").create();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book1 = graph.addVertex(T.label, "book", "name", "Test-Book-1");
        Vertex book2 = graph.addVertex(T.label, "book", "name", "Test-Book-2");
        Vertex book3 = graph.addVertex(T.label, "book", "name", "Test-Book-3");

        james.addEdge("authored", book1,
                      "contribution", "1991 3 1", "score", 5);
        james.addEdge("authored", book2,
                      "contribution", "1992 2 2", "score", 4);
        james.addEdge("authored", book3,
                      "contribution", "1993 3 2", "score", 3);

        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().hasLabel("authored")
                                .has("contribution", Text.contains("1992"))
                                .toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book2,
                       "contribution", "1992 2 2", "score", 4);

        edges = graph.traversal().E().hasLabel("authored")
                                 .has("contribution", Text.contains("2"))
                                 .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().hasLabel("authored")
                                 .has("score", P.gt(3))
                                 .has("contribution", Text.contains("3"))
                                 .toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book1,
                       "contribution", "1991 3 1", "score", 5);
    }

    @Test
    public void testQueryByTextContainsPropertyWithOutEdgeFilter() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authoredByContribution").onE("authored")
             .search().by("contribution").create();
        graph.schema().indexLabel("authoredByScore").onE("authored")
             .range().by("score").create();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book1 = graph.addVertex(T.label, "book", "name", "Test-Book-1");
        Vertex book2 = graph.addVertex(T.label, "book", "name", "Test-Book-2");
        Vertex book3 = graph.addVertex(T.label, "book", "name", "Test-Book-3");

        james.addEdge("authored", book1,
                      "contribution", "1991 3 1", "score", 5);
        james.addEdge("authored", book2,
                      "contribution", "1992 2 2", "score", 4);
        james.addEdge("authored", book3,
                      "contribution", "1993 3 2", "score", 3);

        graph.tx().commit();

        // Won't query by search index, just filter by property after outE()
        List<Edge> edges = graph.traversal().V(james).outE("authored")
                                .has("score", P.gte(4)).toList();
        Assert.assertEquals(2, edges.size());

        // Won't query by search index, just filter by property after outE()
        edges = graph.traversal().V(james).outE("authored")
                                 .has("contribution", "1992 2 2")
                                 .toList();
        Assert.assertEquals(1, edges.size());

        // Won't query by search index, just filter by property after outE()
        edges = graph.traversal().V(james).outE("authored")
                                 .has("contribution", "1992 2 2")
                                 .has("score", P.gte(4))
                                 .toList();
        Assert.assertEquals(1, edges.size());

        // Won't query by search index, just filter by property after outE()
        edges = graph.traversal().V(james).outE("authored")
                                 .has("contribution", "1992 2 2")
                                 .has("score", P.gt(4))
                                 .toList();
        Assert.assertEquals(0, edges.size());

        // Query by search index
        edges = graph.traversal().E()
                     .has("contribution", Text.contains("1992"))
                     .toList();
        Assert.assertEquals(1, edges.size());

        // Won't query by search index, just filter by property after outE()
        edges = graph.traversal().V(james).outE("authored")
                                 .has("contribution", "1992")
                                 .toList();
        Assert.assertEquals(0, edges.size()); // be careful!
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testScanEdge() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan",
                          storeFeatures().supportsScanToken() ||
                          storeFeatures().supportsScanKeyRange());
        init18Edges();

        Set<Edge> edges = new HashSet<>();

        long splitSize = 1 * 1024 * 1024;
        Object splits = graph.graphTransaction()
                             .metadata(HugeType.EDGE_OUT, "splits", splitSize);
        for (Shard split : (List<Shard>) splits) {
            ConditionQuery q = new ConditionQuery(HugeType.EDGE);
            q.scan(split.start(), split.end());
            edges.addAll(ImmutableList.copyOf(graph.edges(q)));
        }

        Assert.assertEquals(18, edges.size());
    }

    @Test
    public void testRemoveEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex java = graph.addVertex(T.label, "language", "name", "java");

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);

        Edge authored1 = james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(4, edges.size());

        authored1.remove();

        edges = graph.traversal().E().toList();
        Assert.assertEquals(3, edges.size());
        Assert.assertFalse(Utils.contains(edges,
                           new FakeEdge("authored", james, java1)));
    }

    @Test
    public void testRemoveEdgeNotExists() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex java = graph.addVertex(T.label, "language", "name", "java");

        Edge created = james.addEdge("created", java);

        created.remove();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(0, edges.size());
        // Remove again
        created.remove();
    }

    @Test
    public void testRemoveEdgeOneByOne() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido =  graph.addVertex(T.label, "author", "id", 2,
                                        "name", "Guido van Rossum", "age", 61,
                                        "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(5, edges.size());

        for (int i = 0; i < edges.size(); i++) {
            edges.get(i).remove();
            Assert.assertEquals(4 - i, graph.traversal().E().toList().size());
        }
    }

    @Test
    public void testRemoveEdgesOfVertex() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido =  graph.addVertex(T.label, "author", "id", 2,
                                        "name", "Guido van Rossum", "age", 61,
                                        "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        guido.addEdge("write", java1, "time", "2017-6-7");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(6, edges.size());

        // It will remove all edges of the vertex
        james.remove();

        edges = graph.traversal().E().toList();
        Assert.assertEquals(2, edges.size());
        assertContains(edges, "created", guido, python);
        assertContains(edges, "write", guido, java1,
                       "time", "2017-6-7");

        edges = graph.traversal().V(java1.id()).inE().toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(java2.id()).inE().toList();
        Assert.assertEquals(0, edges.size());

        edges = graph.traversal().V(java3.id()).inE().toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testRemoveEdgesOfSuperVertex() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido =  graph.addVertex(T.label, "author", "id", 2,
                                        "name", "Guido van Rossum", "age", 61,
                                        "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        // Add some edges
        int txCap = TX_BATCH;
        for (int i = 0; i < txCap / TX_BATCH; i++) {
            for (int j = 0; j < TX_BATCH; j++) {
                int time = i * TX_BATCH + j;
                guido.addEdge("write", java1, "time", "time-" + time);
            }
            graph.tx().commit();
        }

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(txCap + 5, edges.size());

        // It will remove all edges of the vertex
        guido.remove();
        graph.tx().commit();

        edges = graph.traversal().E().toList();
        Assert.assertEquals(4, edges.size());
        assertContains(edges, "created", james, java);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3);

        // Add large amount of edges
        txCap = graph.configuration().get(CoreOptions.EDGE_TX_CAPACITY);
        assert txCap / TX_BATCH > 0 && txCap % TX_BATCH == 0;
        for (int i = 0; i < txCap / TX_BATCH; i++) {
            for (int j = 0; j < TX_BATCH; j++) {
                int time = i * TX_BATCH + j;
                guido.addEdge("write", java1, "time", "time-" + time);
            }
            graph.tx().commit();
        }

        guido.addEdge("created", python);

        edges = graph.traversal().E().toList();
        Assert.assertEquals(txCap + 5, edges.size());

        // It will remove all edges of the vertex
        guido.remove();

        Assert.assertThrows(LimitExceedException.class, () -> {
            graph.tx().commit();
        }, (e) -> {
            Assert.assertTrue(e.getMessage().contains(
                              "Edges size has reached tx capacity"));
        });
    }

    @Test
    public void testRemoveEdgeAfterAddEdgeWithTx() {
        HugeGraph graph = graph();
        GraphTransaction tx = graph.openTransaction();

        Vertex james = tx.addVertex(T.label, "author", "id", 1,
                                    "name", "James Gosling", "age", 62,
                                    "lived", "Canadian");

        Vertex java = tx.addVertex(T.label, "language", "name", "java");

        Edge created = james.addEdge("created", java);
        created.remove();

        try {
            tx.commit();
        } finally {
            tx.close();
        }

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testRemoveVertexAfterAddEdgesWithTx() {
        HugeGraph graph = graph();
        GraphTransaction tx = graph.openTransaction();

        Vertex james = tx.addVertex(T.label, "author", "id", 1,
                                    "name", "James Gosling", "age", 62,
                                    "lived", "Canadian");
        Vertex guido =  tx.addVertex(T.label, "author", "id", 2,
                                     "name", "Guido van Rossum", "age", 61,
                                     "lived", "California");

        Vertex java = tx.addVertex(T.label, "language", "name", "java");
        Vertex python = tx.addVertex(T.label, "language", "name", "python",
                                     "dynamic", true);

        Vertex java1 = tx.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = tx.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = tx.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        james.remove();
        guido.remove();

        try {
            tx.commit();
        } finally {
            tx.close();
        }

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testAddEdgeProperty() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current);
        graph.tx().commit();

        // Add property
        edge.property("message", "Happy birthday!");
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "transfer", louise, sean, "id", 1,
                       "amount", 500.00F, "timestamp", current,
                       "message", "Happy birthday!");
    }

    @Test
    public void testAddEdgePropertyNotInEdgeLabel() {
        Edge edge = initEdgeTransfer();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("age", 18);
        });
    }

    @Test
    public void testAddEdgePropertyWithNotExistPropKey() {
        Edge edge = initEdgeTransfer();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("prop-not-exist", "2017-1-1");
        });
    }

    @Test
    public void testAddEdgePropertyWithIllegalValueForIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        graph.tx().commit();

        long current = System.currentTimeMillis();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            louise.addEdge("strike", sean, "id", 1,
                           "timestamp", current, "place", "park",
                           "tool", "\u0000", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();
        }, (e) -> {
            Assert.assertTrue(e.getMessage().contains(
                              "Illegal value of index property: '\u0000'"));
        });
    }

    @Test
    public void testUpdateEdgeProperty() {
        HugeGraph graph = graph();

        Edge edge = initEdgeTransfer();
        Assert.assertEquals(500.00F, edge.property("amount").value());

        // Update property
        edge.property("amount", 200.00F);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        edge = edges.get(0);
        Assert.assertEquals(200.00F, edge.property("amount").value());
    }

    @Test
    public void testUpdateEdgePropertyWithRemoveAndSet() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current,
                                   "message", "Happy birthday!");
        graph.tx().commit();

        edge.property("message").remove();
        edge.property("message", "Happy birthday ^-^");
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "transfer", louise, sean, "id", 1,
                       "amount", 500.00F, "timestamp", current,
                       "message", "Happy birthday ^-^");
    }

    @Test
    public void testUpdateEdgePropertyTwice() {
        HugeGraph graph = graph();

        Edge edge = initEdgeTransfer();
        Assert.assertEquals(500.00F, edge.property("amount").value());

        edge.property("amount", 100.00F);
        edge.property("amount", 200.00F);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        edge = edges.get(0);
        Assert.assertEquals(200.00F, edge.property("amount").value());
    }

    @Test
    public void testUpdateEdgePropertyOfSortKey() {
        Edge edge = initEdgeTransfer();

        Assert.assertEquals(1, edge.property("id").value());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Update sort key property
            edge.property("id", 2);
        });
    }

    @Test
    public void testUpdateEdgePropertyOfNewEdge() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current,
                                   "message", "Happy birthday!");

        edge.property("amount", 200.00F);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "transfer", louise, sean, "id", 1,
                       "amount", 200.00F, "timestamp", current,
                       "message", "Happy birthday!");
    }

    @Test
    public void testUpdateEdgePropertyOfAddingEdge() {
        Edge edge = initEdgeTransfer();

        Vertex louise = vertex("person", "name", "Louise");
        Vertex sean = vertex("person", "name", "Sean");

        louise.addEdge("transfer", sean, "id", 1, "amount", 500.00F,
                       "timestamp", edge.value("timestamp"),
                       "message", "Happy birthday!");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message", "*");
        });
    }

    @Test
    public void testUpdateEdgePropertyOfRemovingEdge() {
        Edge edge = initEdgeTransfer();

        edge.remove();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message", "*");
        });
    }

    @Test
    public void testUpdateEdgePropertyOfRemovingEdgeWithDrop() {
        HugeGraph graph = graph();
        Edge edge = initEdgeTransfer();

        graph.traversal().E(edge.id()).drop().iterate();

        // Update on dirty vertex
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message", "*");
        });
    }

    @Test
    public void testRemoveEdgeProperty() {
        HugeGraph graph = graph();

        Edge edge = initEdgeTransfer();
        Assert.assertEquals("Happy birthday!",
                            edge.property("message").value());

        // Remove property
        edge.property("message").remove();
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertFalse(edges.get(0).property("message").isPresent());
    }

    @Test
    public void testRemoveEdgePropertyTwice() {
        HugeGraph graph = graph();

        Edge edge = initEdgeTransfer();
        Assert.assertEquals(500.00F, edge.property("amount").value());
        Assert.assertEquals("Happy birthday!",
                            edge.property("message").value());

        // Remove property twice
        edge.property("message").remove();
        edge.property("message").remove();
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertFalse(edges.get(0).property("message").isPresent());
    }

    @Test
    public void testRemoveEdgePropertyOfSortKey() {
        Edge edge = initEdgeTransfer();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("id").remove();
        });
    }

    @Test
    public void testRemoveEdgePropertyNullableWithIndex() {
        HugeGraph graph = graph();
        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);
        edge.property("tool").remove();
        graph.tx().commit();
    }

    @Test
    public void testRemoveEdgePropertyNonNullWithIndex() {
        HugeGraph graph = graph();
        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("timestamp").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("place").remove();
        });
    }

    @Test
    public void testRemoveEdgePropertyNullableWithoutIndex() {
        HugeGraph graph = graph();
        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "hurt", true, "arrested", false);
        edge.property("hurt").remove();
        graph.tx().commit();
    }

    @Test
    public void testRemoveEdgePropertyNonNullWithoutIndex() {
        HugeGraph graph = graph();
        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("arrested").remove();
        });
    }

    @Test
    public void testQueryEdgeByPropertyWithEmptyString() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1,
                       "timestamp", current, "place", "park",
                       "tool", "", "reason", "jeer",
                       "arrested", false);
        Edge edge = graph.traversal().E().has("tool", "").next();
        Assert.assertEquals(1, (int) edge.value("id"));
        Assert.assertEquals("", edge.value("tool"));

        edge = graph.traversal().E().has("tool", "").has("place", "park")
               .has("reason", "jeer").next();
        Assert.assertEquals(1, (int) edge.value("id"));
    }

    @Test
    public void testQueryEdgeBeforeAfterUpdateMultiPropertyWithIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);

        List<Edge> vl = graph.traversal().E().has("tool", "shovel").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals(1, (int) vl.get(0).value("id"));
        vl = graph.traversal().E().has("timestamp", current).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals(1, (int) vl.get(0).value("id"));
        vl = graph.traversal().E().has("tool", "knife").toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().E().has("timestamp", 666L).toList();
        Assert.assertEquals(0, vl.size());

        edge.property("tool", "knife");
        edge.property("timestamp", 666L);

        vl = graph.traversal().E().has("tool", "shovel").toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().E().has("timestamp", current).toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().E().has("tool", "knife").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals(1, (int) vl.get(0).value("id"));
        vl = graph.traversal().E().has("timestamp", 666L).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals(1, (int) vl.get(0).value("id"));
    }

    @Test
    public void testQueryEdgeBeforeAfterUpdatePropertyWithSecondaryIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);

        List<Edge> el = graph.traversal().E().has("tool", "shovel")
                        .toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
        el = graph.traversal().E().has("tool", "knife").toList();
        Assert.assertEquals(0, el.size());

        edge.property("tool", "knife");

        el = graph.traversal().E().has("tool", "shovel").toList();
        Assert.assertEquals(0, el.size());
        el = graph.traversal().E().has("tool", "knife").toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
    }

    @Test
    public void testQueryEdgeBeforeAfterUpdatePropertyWithRangeIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);

        List<Edge> el = graph.traversal().E().has("timestamp", current)
                        .toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
        el = graph.traversal().E().has("timestamp", 666L).toList();
        Assert.assertEquals(0, el.size());

        edge.property("timestamp", 666L);

        el = graph.traversal().E().has("timestamp", current).toList();
        Assert.assertEquals(0, el.size());
        el = graph.traversal().E().has("timestamp", 666L).toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
    }

    @Test
    public void testQueryEdgeWithNullablePropertyInCompositeIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1,
                       "timestamp", current, "place", "park",
                       "tool", "shovel", "arrested", false);

        List<Edge> el = graph.traversal().E().has("place", "park")
                        .toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
        el = graph.traversal().E().has("place", "park")
             .has("tool", "shovel").toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
    }

    @Test
    public void testQueryEdgeByPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        GraphTraversal<Edge, Edge> itor = graph.traversal().E()
                                               .has("~page", "").limit(10);
        Assert.assertEquals(10, IteratorUtils.count(itor));
        String page = TraversalUtil.page(itor);

        List<Edge> edges;

        edges = graph.traversal().E()//.hasLabel("look")
                     .has("~page", page).limit(1)
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().E()
                     .has("~page", page).limit(33)
                     .toList();
        Assert.assertEquals(33, edges.size());

        edges = graph.traversal().E()
                     .has("~page", page).limit(89)
                     .toList();
        Assert.assertEquals(89, edges.size());

        edges = graph.traversal().E()
                     .has("~page", page).limit(91)
                     .toList();
        Assert.assertEquals(90, edges.size());
    }

    @Test
    public void testQueryEdgeByPageResultsMatched() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        List<Edge> all = graph.traversal().E().toList();

        GraphTraversal<Edge, Edge> itor;

        String page = "";
        int size = 20;

        for (int i = 0; i < 100 / size; i++) {
            itor = graph.traversal().E()
                        .has("~page", page).limit(size);
            List<?> vertexes = IteratorUtils.asList(itor);
            Assert.assertEquals(size, vertexes.size());

            List<Edge> expected = all.subList(i * size, (i + 1) * size);
            Assert.assertEquals(expected, vertexes);

            page = TraversalUtil.page(itor);
        }
        Assert.assertNull(page);
    }

    @Test
    public void testQueryEdgeByPageWithInvalidPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        // Illegal base64 character
        Assert.assertThrows(BackendException.class, () -> {
            graph.traversal().E()
                 .has("~page", "!abc123#").limit(10)
                 .toList();
        });

        // Invalid page
        Assert.assertThrows(BackendException.class, () -> {
            graph.traversal().E()
                 .has("~page", "abc123").limit(10)
                 .toList();
        });
    }

    @Test
    public void testQueryEdgeByPageWithInvalidLimit() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        Assert.assertThrows(IllegalStateException.class, () -> {
            graph.traversal().E()
                 .has("~page", "").limit(0)
                 .toList();
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            graph.traversal().E()
                 .has("~page", "").limit(-1)
                 .toList();
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            graph.traversal().E()
                 .has("~page", "")
                 .toList();
        });
    }

    @Test
    public void testQueryEdgeByPageWithOffset() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        Assert.assertThrows(IllegalStateException.class, () -> {
            graph.traversal().E()
                 .has("~page", "").range(2, 10)
                 .toList();
        });
    }

    private void init18Edges() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido =  graph.addVertex(T.label, "author", "id", 2,
                                        "name", "Guido van Rossum", "age", 61,
                                        "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        Vertex selina = graph.addVertex(T.label, "person", "name", "Selina",
                                        "city", "Beijing", "age", 24);

        james.addEdge("created", java);
        guido.addEdge("created", python);

        guido.addEdge("know", james);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3, "score", 3);

        louise.addEdge("look", java1, "time", "2017-5-1");
        louise.addEdge("look", java1, "time", "2017-5-27");
        louise.addEdge("look", java2, "time", "2017-5-27");
        louise.addEdge("look", java3, "time", "2017-5-1", "score", 3);
        jeff.addEdge("look", java3, "time", "2017-5-27", "score", 3);
        sean.addEdge("look", java3, "time", "2017-5-27", "score", 4);
        selina.addEdge("look", java3, "time", "2017-5-27", "score", 0);

        louise.addEdge("friend", jeff);
        louise.addEdge("friend", sean);
        louise.addEdge("friend", selina);
        jeff.addEdge("friend", sean);
        jeff.addEdge("follow", james);

        graph.tx().commit();
    }

    private void init100LookEdges() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);

        Vertex java = graph.addVertex(T.label, "book", "name", "java-book");

        for (int i = 0; i < 50; i++) {
            louise.addEdge("look", java, "time", "time-" + i);
        }

        for (int i = 0; i < 50; i++) {
            jeff.addEdge("look", java, "time", "time-" + i);
        }

        graph.tx().commit();
    }

    private Edge initEdgeTransfer() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current,
                                   "message", "Happy birthday!");

        graph.tx().commit();
        return edge;
    }

    private Vertex vertex(String label, String pkName, Object pkValue) {
        List<Vertex> vertexes = graph().traversal().V()
                                .hasLabel(label)
                                .has(pkName, pkValue).toList();
        Assert.assertEquals(1, vertexes.size());
        return vertexes.get(0);
    }

    private static void assertContains(
            List<Edge> edges,
            String label,
            Vertex outVertex,
            Vertex inVertex,
            Object... kvs) {
        Assert.assertTrue(Utils.contains(edges,
                          new FakeEdge(label, outVertex, inVertex, kvs)));
    }
}
