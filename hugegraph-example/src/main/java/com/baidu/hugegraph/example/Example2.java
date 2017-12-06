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

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.util.Log;

public class Example2 {

    private static final Logger LOG = Log.logger(Example2.class);

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Example2 start!");

        HugeGraph graph = ExampleUtil.loadGraph();

        Example2.load(graph);
        traversal(graph);

        graph.close();
        EventHub.destroy(3);
    }

    public static void traversal(final HugeGraph graph) {

        GraphTraversalSource g = graph.traversal();

        GraphTraversal<Vertex, Vertex> vertexs = g.V();
        System.out.println(">>>> query all vertices: size=" +
                           vertexs.toList().size());

        List<Edge> edges = g.E().toList();
        System.out.println(">>>> query all edges: size=" +
                           edges.size());

        List<Object> names = g.V().inE("knows").limit(2)
                              .outV().values("name").toList();
        System.out.println(">>>> query vertex(with props) of edges: " + names);
        assert names.size() == 2 : names.size();

        names = g.V().as("a")
                 .out("knows")
                 .and()
                 .out("created").in("created").as("a").values("name")
                 .toList();
        System.out.println(">>>> query with AND: " + names);
        assert names.size() == 1 : names.size();

        List<Vertex> vertex = g.V().has("age", 29).toList();
        System.out.println(">>>> age = 29: " + vertex);
        assert vertex.size() == 1 &&
               vertex.get(0).value("name").equals("marko");

        vertex = g.V()
                  .has("age", 29)
                  .has("city", "Beijing")
                  .toList();
        System.out.println(">>>> age = 29 and city is Beijing: " + vertex);
        assert vertex.size() == 1 &&
               vertex.get(0).value("name").equals("marko");

        edges = g.E().has("weight", P.lt(1.0)).toList();
        System.out.println(">>>> edges with weight < 1.0: " + edges);
        assert edges.size() == 4;

        vertex = g.V("person:josh")
                  .bothE("created")
                  .has("weight", P.lt(1.0))
                  .otherV()
                  .toList();
        System.out.println(">>>> josh's both edges with weight < 1.0: " +
                           vertex);
        assert vertex.size() == 1 && vertex.get(0).value("name").equals("lop");

        List<Path> paths = g.V("person:marko")
                            .out().out().path().by("name").toList();
        System.out.println(">>>> test out path: " + paths);
        assert paths.size() == 2;
        assert paths.get(0).get(0).equals("marko");
        assert paths.get(0).get(1).equals("josh");
        assert paths.get(0).get(2).equals("lop");
        assert paths.get(1).get(0).equals("marko");
        assert paths.get(1).get(1).equals("josh");
        assert paths.get(1).get(2).equals("ripple");

        VertexLabel person = graph.schema().getVertexLabel("person");
        VertexLabel software = graph.schema().getVertexLabel("software");
        String markoId = String.format("%s:%s", person.id().asString(),
                                       "marko");
        String lopId = String.format("%s:%s", software.id().asString(), "lop");
        paths = shortestPath(graph, markoId, lopId, 5);
        System.out.println(">>>> test shortest path: " + paths.get(0));
        assert paths.get(0).get(0).equals("marko");
        assert paths.get(0).get(1).equals("lop");
    }

    public static List<Path> shortestPath(final HugeGraph graph,
                                          Object from, Object to,
                                          int maxDepth) {
        GraphTraversal<Vertex, Path> t = graph.traversal()
                .V(from)
                .repeat(__.out().simplePath())
                .until(__.hasId(to).or().loops().is(P.gt(maxDepth)))
                .hasId(to)
                .path().by("name")
                .limit(1);
        return t.toList();
    }

    public static void load(final HugeGraph graph) {
        SchemaManager schema = graph.schema();

        /*
         * NOTE:
         * Use schema.propertyKey interface to create propertyKey.
         * Use schema.propertyKey interface to query propertyKey.
         */
        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("city").asText().ifNotExist().create();
        schema.propertyKey("weight").asDouble().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asText().ifNotExist().create();
        schema.propertyKey("price").asInt().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("age")
              .ifNotExist()
              .create();

        schema.vertexLabel("software")
              .properties("name", "lang", "price")
              .primaryKeys("name")
              .nullableKeys("price")
              .ifNotExist()
              .create();

        schema.indexLabel("personByName")
              .onV("person")
              .by("name")
              .secondary()
              .ifNotExist()
              .create();

        schema.indexLabel("personByCity")
              .onV("person")
              .by("city")
              .secondary()
              .ifNotExist()
              .create();

        schema.indexLabel("personByAgeAndCity")
              .onV("person")
              .by("age", "city")
              .secondary()
              .ifNotExist().create();

        schema.indexLabel("softwareByPrice")
              .onV("software")
              .by("price")
              .search()
              .ifNotExist()
              .create();

        schema.edgeLabel("knows")
              .multiTimes()
              .sourceLabel("person")
              .targetLabel("person")
              .properties("date", "weight")
              .sortKeys("date")
              .nullableKeys("weight")
              .ifNotExist()
              .create();

        schema.edgeLabel("created")
              .sourceLabel("person").targetLabel("software")
              .properties("date", "weight")
              .nullableKeys("weight")
              .ifNotExist()
              .create();

        schema.indexLabel("createdByDate")
              .onE("created")
              .by("date")
              .secondary()
              .ifNotExist()
              .create();

        schema.indexLabel("createdByWeight")
              .onE("created")
              .by("weight")
              .search()
              .ifNotExist()
              .create();

        schema.indexLabel("knowsByWeight")
              .onE("knows")
              .by("weight")
              .search()
              .ifNotExist()
              .create();

        schema.getAllSchema();

        graph.tx().open();

        Vertex marko = graph.addVertex(T.label, "person", "name", "marko",
                                       "age", 29, "city", "Beijing");
        Vertex vadas = graph.addVertex(T.label, "person", "name", "vadas",
                                       "age", 27, "city", "Hongkong");
        Vertex lop = graph.addVertex(T.label, "software", "name", "lop",
                                     "lang", "java", "price", 328);
        Vertex josh = graph.addVertex(T.label, "person", "name", "josh",
                                      "age", 32, "city", "Beijing");
        Vertex ripple = graph.addVertex(T.label, "software", "name", "ripple",
                                        "lang", "java", "price", 199);
        Vertex peter = graph.addVertex(T.label, "person", "name", "peter",
                                       "age", 35, "city", "Shanghai");

        marko.addEdge("knows", vadas, "date", "20160110", "weight", 0.5);
        marko.addEdge("knows", josh, "date", "20130220", "weight", 1.0);
        marko.addEdge("created", lop, "date", "20171210", "weight", 0.4);
        josh.addEdge("created", lop, "date", "20091111", "weight", 0.4);
        josh.addEdge("created", ripple, "date", "20171210", "weight", 1.0);
        peter.addEdge("created", lop, "date", "20170324", "weight", 0.2);

        graph.tx().commit();
    }
}
