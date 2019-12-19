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
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.traversal.optimize.Text;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Log;

public class Example1 {

    private static final Logger LOG = Log.logger(Example1.class);

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Example1 start!");

        HugeGraph graph = ExampleUtil.loadGraph();

        Example1.showFeatures(graph);

        Example1.loadSchema(graph);
        Example1.loadData(graph);
        Example1.testQuery(graph);
//        Example1.testRemove(graph);
//        Example1.testVariables(graph);
//        Example1.testLeftIndexProcess(graph);

        Example1.thread(graph);

        graph.close();

        HugeGraph.shutdown(30L);
    }

    private static void thread(HugeGraph graph) throws InterruptedException {
        Thread t = new Thread(() -> {
            // Default tx
            graph.addVertex(T.label, "book", "name", "java-11");
            graph.addVertex(T.label, "book", "name", "java-12");
            graph.tx().commit();

            // New tx
            GraphTransaction tx = graph.openTransaction();
            tx.addVertex(T.label, "book", "name", "java-21");
            tx.addVertex(T.label, "book", "name", "java-22");
            tx.commit();
            tx.close();

            graph.closeTx(); // this will close the schema tx
        });

        t.start();
        t.join();
    }

    public static void showFeatures(final HugeGraph graph) {
        LOG.info("SupportsPersistence: {}",
                 graph.features().graph().supportsPersistence());
    }

    public static void loadSchema(final HugeGraph graph) {

        SchemaManager schema = graph.schema();

        // Schema changes will be commit directly into the back-end
        LOG.info("===============  propertyKey  ================");
        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("gender").asText().create();
        schema.propertyKey("instructions").asText().create();
        schema.propertyKey("category").asText().create();
        schema.propertyKey("year").asInt().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("timestamp").asDate().create();
        schema.propertyKey("ISBN").asText().create();
        schema.propertyKey("calories").asInt().create();
        schema.propertyKey("amount").asText().create();
        schema.propertyKey("stars").asInt().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueSet().create();
        schema.propertyKey("contribution").asText().valueSet().create();
        schema.propertyKey("nickname").asText().valueList().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("country").asText().valueSet().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("sensor_id").asUUID().create();
        schema.propertyKey("versions").asInt().valueList().create();

        LOG.info("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id").create();
        schema.vertexLabel("language").properties("name", "versions")
              .primaryKeys("name").create();
        schema.vertexLabel("recipe").properties("name", "instructions")
              .primaryKeys("name").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.vertexLabel("reviewer").properties("name", "timestamp")
              .primaryKeys("name").create();

        // vertex label must have the properties that specified in primary key
        schema.vertexLabel("FridgeSensor").properties("city")
              .primaryKeys("city").create();

        LOG.info("===============  vertexLabel & index  ================");
        schema.indexLabel("personByCity")
              .onV("person").secondary().by("city").create();
        schema.indexLabel("personByAge")
              .onV("person").range().by("age").create();

        schema.indexLabel("authorByLived")
              .onV("author").search().by("lived").create();

        // schemaManager.getVertexLabel("author").index("byName").secondary().by("name").add();
        // schemaManager.getVertexLabel("recipe").index("byRecipe").materialized().by("name").add();
        // schemaManager.getVertexLabel("meal").index("byMeal").materialized().by("name").add();
        // schemaManager.getVertexLabel("ingredient").index("byIngredient").materialized().by("name").add();
        // schemaManager.getVertexLabel("reviewer").index("byReviewer").materialized().by("name").add();

        LOG.info("===============  edgeLabel  ================");

        schema.edgeLabel("authored").singleTime()
              .sourceLabel("author").targetLabel("book")
              .properties("contribution", "comment")
              .nullableKeys("comment")
              .create();

        schema.edgeLabel("write").multiTimes().properties("time")
              .sourceLabel("author").targetLabel("book")
              .sortKeys("time")
              .create();

        schema.edgeLabel("look").multiTimes().properties("timestamp")
              .sourceLabel("person").targetLabel("book")
              .sortKeys("timestamp")
              .create();

        schema.edgeLabel("created").singleTime()
              .sourceLabel("author").targetLabel("language")
              .create();

        schema.edgeLabel("rated")
              .sourceLabel("reviewer").targetLabel("recipe")
              .create();
    }

    public static void loadData(final HugeGraph graph) {

        // will auto open tx (would not auto commit)
        graph.addVertex(T.label, "book", "name", "java-3");

        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3);
        graph.addVertex(T.label, "person", "name", "James",
                        "city", "Beijing", "age", 19);
        graph.addVertex(T.label, "person", "name", "Tom Cat",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Lisa",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Hebe",
                        "city", "Taipei", "age", 21);

        graph.tx().commit();

        // must commit manually with new backend tx (independent of tinkerpop)
        GraphTransaction tx = graph.openTransaction();

        LOG.info("===============  addVertex  ================");
        Vertex james = tx.addVertex(T.label, "author", "id", 1,
                                    "name", "James Gosling",  "age", 62,
                                    "lived", "San Francisco Bay Area");

        Vertex java = tx.addVertex(T.label, "language", "name", "java",
                                   "versions", Arrays.asList(6, 7, 8));
        Vertex book1 = tx.addVertex(T.label, "book", "name", "java-1");
        Vertex book2 = tx.addVertex(T.label, "book", "name", "java-2");
        Vertex book3 = tx.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        james.addEdge("authored", book1,
                      "contribution", "1990-1-1",
                      "comment", "it's a good book",
                      "comment", "it's a good book",
                      "comment", "it's a good book too");
        james.addEdge("authored", book2, "contribution", "2017-4-28");

        james.addEdge("write", book2, "time", "2017-4-28");
        james.addEdge("write", book3, "time", "2016-1-1");
        james.addEdge("write", book3, "time", "2017-4-28");

        // commit data changes
        try {
            tx.commit();
        } catch (BackendException e) {
            e.printStackTrace();
            try {
                tx.rollback();
            } catch (BackendException e2) {
                e2.printStackTrace();
            }
        } finally {
            tx.close();
        }

        // use the manually open transaction (tinkerpop tx)
        graph.tx().open();
        graph.addVertex(T.label, "book", "name", "java-3");
        graph.addVertex(T.label, "book", "name", "java-4");
        graph.addVertex(T.label, "book", "name", "java-5");
        graph.tx().commit();
    }

    public static void testQuery(final HugeGraph graph) {
//        System.out.println(">>>>groupCount="+
//                graph.traversal().E().hasLabel("has_vcore_attr")
//                .groupCount().by(__.outV().properties("p_trade").value()).explain());
//
//        System.out.println(">>>>flatmapCount="+
//                graph.traversal().E().hasLabel("has_vcore_attr")
//                .outV().has("p_trade","xx").count().profile());

        System.out.println(">>>>groupCount="+
                graph.traversal().E().hasLabel("write")
                     .groupCount().by(__.bothV().values("name")).toList());

        System.out.println(">>>>groupCount="+
                graph.traversal().E().hasLabel("write")
                     .group().by(__.inV().values("name")).toList());

        System.out.println(">>>>num1="+
                graph.traversal().V().hasLabel("author","book").count().toList());

        System.out.println(">>>>num1.prop="+
                graph.traversal().V().hasLabel("person").has("age", P.gt(18)).count().toList());

        System.out.println(">>>>num1 max="+
                graph.traversal().V().hasLabel("author").count().max().toList());

        System.out.println(">>>>num2="+
                graph.traversal().V().hasLabel("author").bothE().count().toList());

        System.out.println(">>>>num3="+
            graph.traversal().V().hasLabel("person").values("age").sum().toList());

        System.out.println(">>>>num4="+
            graph.traversal().V().hasLabel("person").both().values("age").sum().toList());

        // query all
        GraphTraversal<Vertex, Vertex> vertices = graph.traversal().V();
        int size = vertices.toList().size();
        assert size == 12;
        System.out.println(">>>> query all vertices: size=" + size);

        // query by label
        vertices = graph.traversal().V().hasLabel("person");
        size = vertices.toList().size();
        assert size == 5;
        System.out.println(">>>> query all persons: size=" + size);

        // query vertex by primary-values
        vertices = graph.traversal().V().hasLabel("author").has("id", 1);
        List<Vertex> vertexList = vertices.toList();
        assert vertexList.size() == 1;
        System.out.println(">>>> query vertices by primary-values: " +
                           vertexList);

        VertexLabel author = graph.schema().getVertexLabel("author");
        String authorId = String.format("%s:%s", author.id().asString(), "11");

        // query vertex by id and query out edges
        vertices = graph.traversal().V(authorId);
        GraphTraversal<Vertex, Edge> edgesOfVertex = vertices.outE("created");
        List<Edge> edgeList = edgesOfVertex.toList();
        assert edgeList.size() == 1;
        System.out.println(">>>> query edges of vertex: " + edgeList);

        vertices = graph.traversal().V(authorId);
        vertexList = vertices.out("created").toList();
        assert vertexList.size() == 1;
        System.out.println(">>>> query vertices of vertex: " + vertexList);

        // query edge by sort-values
        vertices = graph.traversal().V(authorId);
        edgesOfVertex = vertices.outE("write").has("time", "2017-4-28");
        edgeList = edgesOfVertex.toList();
        assert edgeList.size() == 2;
        System.out.println(">>>> query edges of vertex by sort-values: " +
                           edgeList);

        // query vertex by condition (filter by property name)
        ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
        PropertyKey age = graph.propertyKey("age");
        q.key(HugeKeys.PROPERTIES, age.id());
        if (graph.graphTransaction().store().features()
                 .supportsQueryWithContainsKey()) {
            Iterator<Vertex> iter = graph.vertices(q);
            assert iter.hasNext();
            System.out.println(">>>> queryVertices(age): " + iter.hasNext());
            while (iter.hasNext()) {
                System.out.println(">>>> queryVertices(age): " + iter.next());
            }
        }

        // query all edges
        GraphTraversal<Edge, Edge> edges = graph.traversal().E().limit(2);
        size = edges.toList().size();
        assert size == 2;
        System.out.println(">>>> query all edges with limit 2: size=" + size);

        // query edge by id
        EdgeLabel authored = graph.edgeLabel("authored");
        VertexLabel book = graph.schema().getVertexLabel("book");
        String book1Id = String.format("%s:%s", book.id().asString(), "java-1");
        String book2Id = String.format("%s:%s", book.id().asString(), "java-2");

        String edgeId = String.format("S%s>%s>%s>S%s",
                                      authorId, authored.id(), "", book2Id);
        edges = graph.traversal().E(edgeId);
        edgeList = edges.toList();
        assert edgeList.size() == 1;
        System.out.println(">>>> query edge by id: " + edgeList);

        Edge edge = edgeList.get(0);
        edges = graph.traversal().E(edge.id());
        edgeList = edges.toList();
        assert edgeList.size() == 1;
        System.out.println(">>>> query edge by id: " + edgeList);

        // query edge by condition
        q = new ConditionQuery(HugeType.EDGE);
        q.eq(HugeKeys.OWNER_VERTEX, IdGenerator.of(authorId));
        q.eq(HugeKeys.DIRECTION, Directions.OUT);
        q.eq(HugeKeys.LABEL, authored.id());
        q.eq(HugeKeys.SORT_VALUES, "");
        q.eq(HugeKeys.OTHER_VERTEX, IdGenerator.of(book1Id));

        Iterator<Edge> edges2 = graph.edges(q);
        assert edges2.hasNext();
        System.out.println(">>>> queryEdges(id-condition): " +
                           edges2.hasNext());
        while (edges2.hasNext()) {
            System.out.println(">>>> queryEdges(id-condition): " +
                               edges2.next());
        }

        // NOTE: query edge by has-key just supported by Cassandra
        if (graph.graphTransaction().store().features()
                 .supportsQueryWithContainsKey()) {
            PropertyKey contribution = graph.propertyKey("contribution");
            q.key(HugeKeys.PROPERTIES, contribution.id());
            Iterator<Edge> edges3 = graph.edges(q);
            assert edges3.hasNext();
            System.out.println(">>>> queryEdges(contribution): " +
                               edges3.hasNext());
            while (edges3.hasNext()) {
                System.out.println(">>>> queryEdges(contribution): " +
                                   edges3.next());
            }
        }

        // query by vertex label
        vertices = graph.traversal().V().hasLabel("book");
        size = vertices.toList().size();
        assert size == 5;
        System.out.println(">>>> query all books: size=" + size);

        // query by vertex label and key-name
        vertices = graph.traversal().V().hasLabel("person").has("age");
        size = vertices.toList().size();
        assert size == 5;
        System.out.println(">>>> query all persons with age: size=" + size);

        // query by vertex props
        vertices = graph.traversal().V().hasLabel("person")
                        .has("city", "Taipei");
        vertexList = vertices.toList();
        assert vertexList.size() == 1;
        System.out.println(">>>> query all persons in Taipei: " + vertexList);

        vertices = graph.traversal().V().hasLabel("person").has("age", 19);
        vertexList = vertices.toList();
        assert vertexList.size() == 1;
        System.out.println(">>>> query all persons age==19: " + vertexList);

        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.lt(19));
        vertexList = vertices.toList();
        assert vertexList.size() == 1;
        assert vertexList.get(0).property("age").value().equals(3);
        System.out.println(">>>> query all persons age<19: " + vertexList);

        String addr = "Bay Area";
        vertices = graph.traversal().V().hasLabel("author")
                        .has("lived", Text.contains(addr));
        vertexList = vertices.toList();
        assert vertexList.size() == 1;
        System.out.println(String.format(">>>> query all authors lived %s: %s",
                           addr, vertexList));
    }

    public static void testRemove(final HugeGraph graph) {
        // remove vertex (and its edges)
        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", 19).toList();
        assert vertices.size() == 1;
        Vertex james = vertices.get(0);
        Vertex book6 = graph.addVertex(T.label, "book", "name", "java-6");
        james.addEdge("look", book6, "timestamp", "2017-5-2 12:00:08.0");
        james.addEdge("look", book6, "timestamp", "2017-5-3 12:00:08.0");
        graph.tx().commit();
        assert graph.traversal().V(book6.id()).bothE().hasNext();
        System.out.println(">>>> removing vertex: " + james);
        james.remove();
        graph.tx().commit();
        assert !graph.traversal().V(james.id()).hasNext();
        assert !graph.traversal().V(book6.id()).bothE().hasNext();

        // remove edge
        VertexLabel author = graph.schema().getVertexLabel("author");
        String authorId = String.format("%s:%s", author.id().asString(), "11");
        EdgeLabel authored = graph.edgeLabel("authored");
        VertexLabel book = graph.schema().getVertexLabel("book");
        String book2Id = String.format("%s:%s", book.id().asString(), "java-2");

        String edgeId = String.format("S%s>%s>%s>S%s",
                                      authorId, authored.id(), "", book2Id);

        List <Edge> edges = graph.traversal().E(edgeId).toList();
        assert edges.size() == 1;
        Edge edge = edges.get(0);
        System.out.println(">>>> removing edge: " + edge);
        edge.remove();
        graph.tx().commit();
        assert !graph.traversal().E(edgeId).hasNext();
    }

    public static void testVariables(final HugeGraph graph) {
        // variables test
        Graph.Variables variables = graph.variables();
        variables.set("owner", "zhangyi");
        variables.set("time", 3);
        variables.set("owner", "zhangyi1");
        variables.keys();
        graph.tx().commit();
        variables.remove("time");
        variables.get("time");
        variables.get("owner");
        variables.remove("owner");
        variables.get("owner");
    }

    public static void testLeftIndexProcess(final HugeGraph graph) {
        // test for process left index when addVertex to override prior vertex
        graph.schema().indexLabel("personByCityAndAge").by("city", "age")
             .onV("person").ifNotExist().create();

        ExampleUtil.waitAllTaskDone(graph);

        graph.addVertex(T.label, "person", "name", "Curry",
                        "city", "Hangzhou", "age", 27);
        graph.addVertex(T.label, "person", "name", "Curry",
                        "city", "Shanghai", "age", 28);
        graph.addVertex(T.label, "person", "name", "Curry",
                        "city", "Shanghai", "age", 30);
        graph.tx().commit();

        // set breakpoint here to see secondary_indexes and range_indexes table
        List<Vertex> vertices = graph.traversal().V().has("age", 27)
                                     .has("city", "Hangzhou").toList();
        assert vertices.isEmpty();
        // set breakpoint here to see secondary_indexes and range_indexes table
        vertices = graph.traversal().V().has("age", 28).toList();
        assert vertices.isEmpty();
        // set breakpoint here to see secondary_indexes and range_indexes table
        vertices = graph.traversal().V().has("city", "Hangzhou").toList();
        assert vertices.isEmpty();
    }
}
