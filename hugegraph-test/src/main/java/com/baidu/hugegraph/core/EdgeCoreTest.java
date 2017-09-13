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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.core.FakeObjects.FakeEdge;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Shard;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class EdgeCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        LOG.info("===============  propertyKey  ================");

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

        LOG.info("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id")
              .create();
        schema.vertexLabel("language")
              .properties("name", "dynamic")
              .primaryKeys("name")
              .nullableKeys("dynamic")
              .create();
        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        LOG.info("===============  vertexLabel index  ================");

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").search()
              .by("age").create();

        LOG.info("===============  edgeLabel  ================");

        schema.edgeLabel("transfer")
              .properties("id", "amount", "timestamp", "message")
              .nullableKeys("message")
              .multiTimes().sortKeys("id")
              .link("person", "person")
              .create();
        schema.edgeLabel("authored").singleTime()
              .properties("contribution", "comment", "score")
              .nullableKeys("score", "contribution", "comment")
              .link("author", "book")
              .create();
        schema.edgeLabel("write").properties("time")
              .multiTimes().sortKeys("time")
              .link("author", "book")
              .create();
        schema.edgeLabel("look").properties("time", "score")
              .nullableKeys("score")
              .multiTimes().sortKeys("time")
              .link("person", "book")
              .create();
        schema.edgeLabel("know").singleTime()
              .link("author", "author")
              .create();
        schema.edgeLabel("followedBy").singleTime()
              .link("author", "person")
              .create();
        schema.edgeLabel("friend").singleTime()
              .link("person", "person")
              .create();
        schema.edgeLabel("follow").singleTime()
              .link("person", "author")
              .create();
        schema.edgeLabel("created").singleTime()
              .link("author", "language")
              .create();

        LOG.info("===============  edgeLabel index  ================");

        schema.indexLabel("transferByTimestamp").onE("transfer").search()
              .by("timestamp").create();

        // TODO: add edge index test
        // schema.indexLabel("authoredByScore").on(authored).secondary()
        //       .by("score").create();
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

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3, "score", 4);

        james.addEdge("authored", java1);
        james.addEdge("authored", java3, "score", 5);

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
        assertContains(edges,"authored", james, book,
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
    public void testAddEdgeWithoutSortValues() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("look", book);
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
    public void testQueryEdgesWithLimit() {
        HugeGraph graph = graph();
        init18Edges();
        List<Edge> edges = graph.traversal().E().limit(10).toList();

        Assert.assertEquals(10, edges.size());
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
        Assert.assertThrows(NotFoundException.class, () -> {
            graph.traversal().E(id).next();
        });
    }

    @Test
    public void testQueryEdgesByInvalidId() {
        HugeGraph graph = graph();
        init18Edges();

        String id = "invalid-id";
        Assert.assertTrue(graph.traversal().E(id).toList().isEmpty());
        Assert.assertThrows(NotFoundException.class, () -> {
            graph.traversal().E(id).next();
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
    public void testQueryOutEdgesOfVertexBySortValues() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex louise = vertex("person", "name", "Louise");

        List<Edge> edges = graph.traversal().V(louise.id())
                           .outE("look").has("time", "2017-5-1").toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(louise.id())
                .outE("look").has("time", "2017-5-27").toList();
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
    public void testQueryInEdgesOfVertexBySortValues() {
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

    @SuppressWarnings("unchecked")
    @Test
    public void testScanEdge() {
        HugeGraph graph = graph();
        init18Edges();

        Set<Edge> edges = new HashSet<>();

        long splitSize = 1 * 1024 * 1024;
        Object splits = graph.graphTransaction()
                        .metadata(HugeType.EDGE, "splits", splitSize);
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
    public void testAddEdgeProperty() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current);

        // Add property
        edge.property("message", "Happy birthday!");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "transfer", louise, sean, "id", 1,
                       "amount", 500.00F, "timestamp", current,
                       "message", "Happy birthday!");
    }

    @Test
    public void testAddEdgePropertyExisted() {
        HugeGraph graph = graph();

        Edge edge = initEdgeTransfer();
        Assert.assertEquals(500.00F, edge.property("amount").value());

        edge.property("amount", 200.00F);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        edge = edges.get(0);
        Assert.assertEquals(200.00F, edge.property("amount").value());
    }

    @Test
    public void testAddEdgePropertyNotInEdgeLabel() {
        Edge edge = initEdgeTransfer();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("time", "2017-1-1");
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
    public void testAddEdgePropertyOfSortKey() {
        Edge edge = initEdgeTransfer();

        Assert.assertEquals(1, edge.property("id").value());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Update sort key property
            edge.property("id", 2);
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
