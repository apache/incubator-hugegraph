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

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.util.Log;

public class Example3 {

    private static final Logger LOG = Log.logger(Example3.class);

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Example3 start!");

        HugeGraph graph = ExampleUtil.loadGraph();

        Example3.load(graph);

        graph.close();

        HugeGraph.shutdown(30L);
    }

    public static void load(final HugeGraph graph) {
        SchemaManager schema = graph.schema();

        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("city").asText().ifNotExist().create();
        schema.propertyKey("weight").asDouble().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asText().ifNotExist().create();
        schema.propertyKey("price").asInt().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name")
              .useCustomizeStringId()
              .ifNotExist()
              .create();

        schema.vertexLabel("movie")
              .properties("name")
              .useCustomizeStringId()
              .ifNotExist()
              .create();

        schema.edgeLabel("follow")
              .sourceLabel("person")
              .targetLabel("person")
              .ifNotExist()
              .create();

        schema.edgeLabel("like")
              .sourceLabel("person")
              .targetLabel("movie")
              .ifNotExist()
              .create();

        schema.edgeLabel("directedby")
              .sourceLabel("movie")
              .targetLabel("person")
              .ifNotExist()
              .create();

        graph.tx().open();

        Vertex O = graph.addVertex(T.label, "person", T.id, "O", "name", "O");

        Vertex A = graph.addVertex(T.label, "person", T.id, "A", "name", "A");
        Vertex B = graph.addVertex(T.label, "person", T.id, "B", "name", "B");
        Vertex C = graph.addVertex(T.label, "person", T.id, "C", "name", "C");
        Vertex D = graph.addVertex(T.label, "person", T.id, "D", "name", "D");

        Vertex E = graph.addVertex(T.label, "movie", T.id, "E", "name", "E");
        Vertex F = graph.addVertex(T.label, "movie", T.id, "F", "name", "F");
        Vertex G = graph.addVertex(T.label, "movie", T.id, "G", "name", "G");
        Vertex H = graph.addVertex(T.label, "movie", T.id, "H", "name", "H");
        Vertex I = graph.addVertex(T.label, "movie", T.id, "I", "name", "I");
        Vertex J = graph.addVertex(T.label, "movie", T.id, "J", "name", "J");

        Vertex K = graph.addVertex(T.label, "person", T.id, "K", "name", "K");
        Vertex L = graph.addVertex(T.label, "person", T.id, "L", "name", "L");
        Vertex M = graph.addVertex(T.label, "person", T.id, "M", "name", "M");

        O.addEdge("follow", A);
        O.addEdge("follow", B);
        O.addEdge("follow", C);
        D.addEdge("follow", O);

        A.addEdge("follow", B);
        A.addEdge("like", E);
        A.addEdge("like", F);

        B.addEdge("like", G);
        B.addEdge("like", H);

        C.addEdge("like", I);
        C.addEdge("like", J);

        E.addEdge("directedby", K);
        F.addEdge("directedby", B);
        F.addEdge("directedby", L);

        G.addEdge("directedby", M);

        graph.tx().commit();
    }
}
