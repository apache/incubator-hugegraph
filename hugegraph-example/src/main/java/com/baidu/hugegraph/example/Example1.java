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

import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import com.baidu.hugegraph.util.Log;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;

public class Example1 {

    private static final Logger LOG = Log.logger(Example1.class);

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Example1 start!");

        HugeGraph graph = ExampleUtil.loadGraph();

        Example1.showFeatures(graph);
        Example1.load(graph);

        Example1.thread(graph);

        graph.close();
        System.exit(0);
    }

    private static void thread(HugeGraph graph) throws InterruptedException {
        Thread t = new Thread(() -> {
            // TODO: add graph.initTx()
            graph.addVertex(T.label, "book", "name", "java-11");
            graph.addVertex(T.label, "book", "name", "java-12");
            // TODO: add graph.destroyTx()
            graph.close(); // close current thread tx/store

            GraphTransaction tx = graph.openTransaction();
            tx.addVertex(T.label, "book", "name", "java-21");
            tx.addVertex(T.label, "book", "name", "java-22");
            tx.close(); // this will cause the schema tx not to be closed!
            graph.close(); // this will close the schema tx
        });

        t.start();
        t.join();
    }

    public static void showFeatures(final HugeGraph graph) {
        LOG.info("SupportsPersistence: {}",
                 graph.features().graph().supportsPersistence());
    }

    public static void load(final HugeGraph graph) {

        /*********************** schemaManager operating *********************/
        SchemaManager schema = graph.schema();
        LOG.info("===============  propertyKey  ================");
        schema.propertyKey("id").asInt().create();
        schema.propertyKey("~exist").asText().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("gender").asText().create();
        schema.propertyKey("instructions").asText().create();
        schema.propertyKey("category").asText().create();
        schema.propertyKey("year").asInt().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("timestamp").asTimestamp().create();
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
        schema.propertyKey("sensor_id").asUuid().create();

        LOG.info("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id").create();
        schema.vertexLabel("language").properties("name")
              .primaryKeys("name").create();
        schema.vertexLabel("recipe").properties("name", "instructions")
              .primaryKeys("name").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.vertexLabel("reviewer").properties("name")
              .primaryKeys("name").create();
        // vertex label must have the properties that specified in primary key
        schema.vertexLabel("FridgeSensor").properties("city")
              .primaryKeys("city").create();

        LOG.info("===============  vertexLabel & index  ================");
        schema.indexLabel("personByCity")
              .onV("person").secondary().by("city").create();
        schema.indexLabel("personByAge")
              .onV("person").search().by("age").create();
        // schemaManager.getVertexLabel("author").index("byName").secondary().by("name").add();
        // schemaManager.getVertexLabel("recipe").index("byRecipe").materialized().by("name").add();
        // schemaManager.getVertexLabel("meal").index("byMeal").materialized().by("name").add();
        // schemaManager.getVertexLabel("ingredient").index("byIngredient").materialized().by("name").add();
        // schemaManager.getVertexLabel("reviewer").index("byReviewer").materialized().by("name").add();

        LOG.info("===============  edgeLabel  ================");

        schema.edgeLabel("authored").singleTime()
              .sourceLabel("author").targetLabel("book")
              .properties("contribution", "comment")
              .create();

        schema.edgeLabel("write").multiTimes().properties("time")
              .sourceLabel("author").targetLabel("book")
              .sortKeys("time")
              .create();

        schema.edgeLabel("look").multiTimes().properties("time")
              .sourceLabel("person").targetLabel("book")
              .sortKeys("time")
              .create();

        schema.edgeLabel("created").singleTime()
              .sourceLabel("author").targetLabel("language")
              .create();

        schema.edgeLabel("rated")
              .sourceLabel("reviewer").targetLabel("recipe")
              .create();

        LOG.info("===============  schemaManager desc  ================");
        schema.desc().forEach(element -> System.out.println(element.schema()));

        /************************* data operating *************************/

        // Directly into the back-end
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

        // Must commit manually
        GraphTransaction tx = graph.openTransaction();

        LOG.info("===============  addVertex  ================");
        Vertex james = tx.addVertex(T.label, "author",
                                    "id", 1, "name", "James Gosling",
                                    "age", 62, "lived", "Canadian");

        Vertex java = tx.addVertex(T.label, "language", "name", "java");
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
                // TODO Auto-generated catch block
                e2.printStackTrace();
            }
        } finally {
            tx.close();
        }

        // use the default Transaction to commit
        graph.addVertex(T.label, "book", "name", "java-3");

        // tinkerpop tx
        graph.tx().open();
        graph.addVertex(T.label, "book", "name", "java-4");
        graph.addVertex(T.label, "book", "name", "java-5");
        graph.tx().commit();
        graph.tx().close();

        // query all
        GraphTraversal<Vertex, Vertex> vertexes = graph.traversal().V();
        int size = vertexes.toList().size();
        assert size == 12;
        System.out.println(">>>> query all vertices: size=" + size);

        // query vertex by primary-values
        vertexes = graph.traversal().V().hasLabel("author").has("id", "1");
        List<Vertex> vertexList = vertexes.toList();
        assert vertexList.size() == 1;
        System.out.println(">>>> query vertices by primary-values: " +
                           vertexList);

        // query vertex by id and query out edges
        vertexes = graph.traversal().V("author:1");
        GraphTraversal<Vertex, Edge> edgesOfVertex = vertexes.outE("created");
        List<Edge> edgeList = edgesOfVertex.toList();
        assert edgeList.size() == 1;
        System.out.println(">>>> query edges of vertex: " + edgeList);

        vertexes = graph.traversal().V("author:1");
        GraphTraversal<Vertex, Vertex> verticesOfVertex = vertexes.out("created");
        vertexList = verticesOfVertex.toList();
        assert vertexList.size() == 1;
        System.out.println(">>>> query vertices of vertex: " + vertexList);

        // query edge by sort-values
        vertexes = graph.traversal().V("author:1");
        edgesOfVertex = vertexes.outE("write").has("time", "2017-4-28");
        edgeList = edgesOfVertex.toList();
        assert edgeList.size() == 2;
        System.out.println(">>>> query edges of vertex by sort-values: " +
                           edgeList);

        // query vertex by condition (filter by property name)
        ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
        q.query(IdGenerator.of("author:1"));
        // TODO: remove the PROPERTIES which may just be used by Cassandra
        q.key(HugeKeys.PROPERTIES, "age");

        Iterator<Vertex> vertices = graph.vertices(q);
        assert vertices.hasNext();
        System.out.println(">>>> queryVertices(age): " + vertices.hasNext());
        while (vertices.hasNext()) {
            System.out.println(">>>> queryVertices(age): " + vertices.next());
        }

        // query all edges
        GraphTraversal<Edge, Edge> edges = graph.traversal().E().limit(2);
        size = edges.toList().size();
        assert size == 2;
        System.out.println(">>>> query all edges with limit 2: size=" + size);

        // query edge by id
        String id = "author:1>authored>>book:java-2";
        edges = graph.traversal().E(id);
        edgeList = edges.toList();
        assert edgeList.size() == 1;
        System.out.println(">>>> query edge by id: " + edgeList);

        Edge edge = graph.traversal().E(id).toList().get(0);
        edges = graph.traversal().E(edge.id());
        edgeList = edges.toList();
        assert edgeList.size() == 1;
        System.out.println(">>>> query edge by id: " + edgeList);

        // query edge by condition
        q = new ConditionQuery(HugeType.EDGE);
        q.eq(HugeKeys.SOURCE_VERTEX, "author:1");
        q.eq(HugeKeys.DIRECTION, Direction.OUT);
        q.eq(HugeKeys.LABEL, "authored");
        q.eq(HugeKeys.SORT_VALUES, "");
        q.eq(HugeKeys.TARGET_VERTEX, "book:java-1");
        // NOTE: query edge by has-key just supported by Cassandra
        // q.hasKey(HugeKeys.PROPERTIES, "contribution");

        Iterator<Edge> edges2 = graph.edges(q);
        assert edges2.hasNext();
        System.out.println(">>>> queryEdges(contribution): " + edges2.hasNext());
        while (edges2.hasNext()) {
            System.out.println(">>>> queryEdges(contribution): " + edges2.next());
        }

        // query by vertex label
        vertexes = graph.traversal().V().hasLabel("book");
        size = vertexes.toList().size();
        assert size == 5;
        System.out.println(">>>> query all books: size=" + size);

        // query by vertex label and key-name
        vertexes = graph.traversal().V().hasLabel("person").has("age");
        size = vertexes.toList().size();
        assert size == 5;
        System.out.println(">>>> query all persons with age: size=" + size);

        // query by vertex props
        vertexes = graph.traversal().V().hasLabel("person").has("city", "Taipei");
        vertexList = vertexes.toList();
        assert vertexList.size() == 1;
        System.out.println(">>>> query all persons in Taipei: " + vertexList);

        vertexes = graph.traversal().V().hasLabel("person").has("age", 19);
        vertexList = vertexes.toList();
        assert vertexList.size() == 1;
        System.out.println(">>>> query all persons age==19: " + vertexList);

        vertexes = graph.traversal().V().hasLabel("person").has("age", P.lt(19));
        vertexList = vertexes.toList();
        assert vertexList.size() == 1;
        assert vertexList.get(0).property("age").value().equals(3);
        System.out.println(">>>> query all persons age<19: " + vertexList);

        // remove vertex (and its edges)
        vertexes = graph.traversal().V().hasLabel("person").has("age", 19);
        Vertex vertex = vertexes.toList().get(0);
        vertex.addEdge("look", book3, "time", "2017-5-3");
        System.out.println(">>>> remove vertex: " + vertex);
        vertex.remove();
        try {
            graph.traversal().V(vertex.id()).toList();
            assert false;
        } catch (NotFoundException e) {
            assert e.getMessage().contains("Not found the VERTEX entry");
        }

        // remove edge
        id = "author:1>authored>>book:java-2";
        edges = graph.traversal().E(id);
        edge = edges.toList().get(0);
        System.out.println(">>>> remove edge: " + edge);
        edge.remove();
        try {
            graph.traversal().E(id).toList();
            assert false;
        } catch (NotFoundException e) {
            assert e.getMessage().contains("Not found the EDGE entry");
        }
    }
}
