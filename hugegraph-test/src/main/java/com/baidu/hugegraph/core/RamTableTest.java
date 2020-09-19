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

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.ram.RamTable;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.type.define.Directions;

public class RamTableTest extends BaseCoreTest {

    // max value is 4 billion
    private static final int VERTEX_SIZE = 10000000;
    private static final int EDGE_SIZE = 20000000;

    @Before
    public void initSchema() {
        HugeGraph graph = this.graph();

        graph.schema().vertexLabel("vl1").useCustomizeNumberId().create();
        graph.schema().vertexLabel("vl2").useCustomizeNumberId().create();
        graph.schema().edgeLabel("el1")
                      .sourceLabel("vl1")
                      .targetLabel("vl1")
                      .create();
        graph.schema().edgeLabel("el2")
                      .sourceLabel("vl2")
                      .targetLabel("vl2")
                      .create();
    }

    @Test
    public void testAddAndQuery() throws Exception {
        HugeGraph graph = this.graph();
        int el1 = (int) graph.edgeLabel("el1").id().asLong();
        int el2 = (int) graph.edgeLabel("el2").id().asLong();

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);
        long oldSize = table.edgesSize();
        // insert edges
        for (int i = 0; i < VERTEX_SIZE; i++) {
            table.addEdge(true, i, i, Directions.OUT, el1);
            Assert.assertEquals(oldSize + 2 * i + 1, table.edgesSize());

            table.addEdge(false, i, i + 1, Directions.IN, el2);
            Assert.assertEquals(oldSize + 2 * i + 2, table.edgesSize());
        }

        // query by BOTH
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.BOTH, 0);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge2 = edges.next();
            Assert.assertEquals(i, edge2.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge2.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge2.direction());
            Assert.assertEquals("el2", edge2.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by OUT
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.OUT, el1);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by IN
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.IN, el2);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge1.direction());
            Assert.assertEquals("el2", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query by BOTH & label 1
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.BOTH, el1);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by BOTH & label 2
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.BOTH, el2);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge1.direction());
            Assert.assertEquals("el2", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query non-exist vertex
        Iterator<HugeEdge> edges = table.query(VERTEX_SIZE, Directions.BOTH, 0);
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddAndQueryWithoutAdjEdges() throws Exception {
        HugeGraph graph = this.graph();
        int el1 = (int) graph.edgeLabel("el1").id().asLong();
        int el2 = (int) graph.edgeLabel("el2").id().asLong();

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);
        long oldSize = table.edgesSize();
        // insert edges
        for (int i = 0; i < VERTEX_SIZE; i++) {
            if (i % 3 != 0) {
                // don't insert edges for 2/3 vertices
                continue;
            }

            table.addEdge(true, i, i, Directions.OUT, el1);
            Assert.assertEquals(oldSize + i + 1, table.edgesSize());

            table.addEdge(false, i, i, Directions.OUT, el2);
            Assert.assertEquals(oldSize + i + 2, table.edgesSize());

            table.addEdge(false, i, i + 1, Directions.IN, el2);
            Assert.assertEquals(oldSize + i + 3, table.edgesSize());
        }

        // query by BOTH
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.BOTH, 0);

            if (i % 3 != 0) {
                Assert.assertFalse(edges.hasNext());
                continue;
            }

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge2 = edges.next();
            Assert.assertEquals(i, edge2.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge2.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge2.direction());
            Assert.assertEquals("el2", edge2.label());

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge3 = edges.next();
            Assert.assertEquals(i, edge3.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge3.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge3.direction());
            Assert.assertEquals("el2", edge3.label());

            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testReloadAndQuery() throws Exception {
        HugeGraph graph = this.graph();

        // insert vertices and edges
        for (int i = 0; i < 100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl1", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl1", T.id, i + 100);
            v1.addEdge("el1", v2);
        }
        graph.tx().commit();

        for (int i = 1000; i < 1100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl2", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl2", T.id, i + 100);
            v1.addEdge("el2", v2);
        }
        graph.tx().commit();

        // reload ramtable
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph);

        // query edges
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testReloadAndQueryWithMultiEdges() throws Exception {
        HugeGraph graph = this.graph();

        // insert vertices and edges
        for (int i = 0; i < 100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl1", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl1", T.id, i + 100);
            Vertex v3 = graph.addVertex(T.label, "vl1", T.id, i + 200);
            v1.addEdge("el1", v2);
            v1.addEdge("el1", v3);
            v3.addEdge("el1", v1);
        }
        graph.tx().commit();

        for (int i = 1000; i < 1100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl2", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl2", T.id, i + 100);
            Vertex v3 = graph.addVertex(T.label, "vl2", T.id, i + 200);
            v1.addEdge("el2", v2);
            v1.addEdge("el2", v3);
            v2.addEdge("el2", v3);
        }
        graph.tx().commit();

        // reload ramtable
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph);

        // query edges by OUT
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query edges by BOTH
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.BOTH, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query edges by IN
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.IN, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query edges by OUT
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query edges by BOTH
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.BOTH, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query edges by IN
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.IN, null);
            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testReloadAndQueryWithBigVertex() throws Exception {
        HugeGraph graph = this.graph();

        // only enable this test when ram > 20G
        boolean enableBigRamTest = false;
        long big1 = 2400000000L;
        long big2 = 4200000000L;
        if (!enableBigRamTest) {
            big1 = 100L;
            big2 = 1000L;
        }

        // insert vertices and edges
        for (int i = 0; i < 100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl1", T.id, i + big1);
            Vertex v2 = graph.addVertex(T.label, "vl1", T.id, i + big1 + 100);
            v1.addEdge("el1", v2);
        }
        graph.tx().commit();

        for (int i = 0; i < 100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl2", T.id, i + big2);
            Vertex v2 = graph.addVertex(T.label, "vl2", T.id, i + big2);
            v1.addEdge("el2", v2);
        }
        graph.tx().commit();

        // reload ramtable
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph);

        // query edges
        for (int i = 0; i < 100; i++) {
            long source = i + big1;
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(source),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(source,
                                edge.id().ownerVertexId().asLong());
            Assert.assertEquals(i + big1 + 100,
                                edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        for (int i = 0; i < 100; i++) {
            long source = i + big2;
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(source),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(source,
                                edge.id().ownerVertexId().asLong());
            Assert.assertEquals(i + big2,
                                edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testReloadAndQueryWithProperty() throws Exception {
        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("name")
              .asText()
              .create();
        schema.vertexLabel("person")
              .properties("name")
              .useCustomizeNumberId()
              .create();
        schema.edgeLabel("next")
              .sourceLabel("person")
              .targetLabel("person")
              .properties("name")
              .create();

        GraphTraversalSource g = graph.traversal();
        g.addV("person").property(T.id, 1).property("name", "A").as("a")
         .addV("person").property(T.id, 2).property("name", "B").as("b")
         .addV("person").property(T.id, 3).property("name", "C").as("c")
         .addV("person").property(T.id, 4).property("name", "D").as("d")
         .addV("person").property(T.id, 5).property("name", "E").as("e")
         .addV("person").property(T.id, 6).property("name", "F").as("f")
         .addE("next").from("a").to("b").property("name", "ab")
         .addE("next").from("b").to("c").property("name", "bc")
         .addE("next").from("b").to("d").property("name", "bd")
         .addE("next").from("c").to("d").property("name", "cd")
         .addE("next").from("c").to("e").property("name", "ce")
         .addE("next").from("d").to("e").property("name", "de")
         .addE("next").from("e").to("f").property("name", "ef")
         .addE("next").from("f").to("d").property("name", "fd")
         .iterate();
        graph.tx().commit();

        // reload ramtable
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph);

        GraphTraversal<Vertex, Vertex> vertices;
        HugeVertex vertex;
        GraphTraversal<Vertex, Edge> edges;
        HugeEdge edge;

        // A
        vertices = g.V(1).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertFalse(vertex.propLoaded());
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(1).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertFalse(edge.propLoaded());
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ab", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(1).in();
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(1).inE();
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(1).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(1).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ab", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // B
        vertices = g.V(2).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(2).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("bc", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("bd", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(2).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(1L, vertex.id().asObject());
        Assert.assertEquals("A", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(2).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ab", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(2).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(1L, vertex.id().asObject());
        Assert.assertEquals("A", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(2).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("bc", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("bd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ab", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // C
        vertices = g.V(3).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(3).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("cd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ce", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(3).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(3).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("bc", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(3).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(3).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("cd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ce", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("bc", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // D
        vertices = g.V(4).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(4).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("de", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(4).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(6L, vertex.id().asObject());
        Assert.assertEquals("F", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(4).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("bd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("cd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("fd", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(4).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(6L, vertex.id().asObject());
        Assert.assertEquals("F", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(4).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("de", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("bd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("cd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("fd", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // E
        vertices = g.V(5).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(6L, vertex.id().asObject());
        Assert.assertEquals("F", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(5).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ef", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(5).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(5).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ce", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("de", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(5).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(6L, vertex.id().asObject());
        Assert.assertEquals("F", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(5).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ef", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ce", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("de", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // F
        vertices = g.V(6).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(6).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("fd", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(6).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(6).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ef", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(6).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(6).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("fd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ef", edge.value("name"));
        Assert.assertFalse(edges.hasNext());
    }

    private Iterator<Edge> edgesOfVertex(Id source, Directions dir, Id label) {
        Id[] labels = {};
        if (label != null) {
            labels = new Id[]{label};
        }

        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        return this.graph().edges(query);
    }
}
