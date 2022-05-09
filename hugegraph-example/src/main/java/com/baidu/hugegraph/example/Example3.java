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

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.util.Log;

public class Example3 {

    private static final Logger LOG = Log.logger(Example3.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Example3 start!");

        HugeGraph graph = ExampleUtil.loadGraph();

        Example3.loadNeighborRankData(graph);
        Example3.loadPersonalRankData(graph);

        graph.close();

        HugeFactory.shutdown(30L);
    }

    public static void loadNeighborRankData(final HugeGraph graph) {
        SchemaManager schema = graph.schema();

        schema.propertyKey("name").asText().ifNotExist().create();

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

        schema.edgeLabel("directedBy")
              .sourceLabel("movie")
              .targetLabel("person")
              .ifNotExist()
              .create();

        graph.tx().open();

        Vertex o = graph.addVertex(T.label, "person", T.id, "O", "name", "O");

        Vertex a = graph.addVertex(T.label, "person", T.id, "A", "name", "A");
        Vertex b = graph.addVertex(T.label, "person", T.id, "B", "name", "B");
        Vertex c = graph.addVertex(T.label, "person", T.id, "C", "name", "C");
        Vertex d = graph.addVertex(T.label, "person", T.id, "D", "name", "D");

        Vertex e = graph.addVertex(T.label, "movie", T.id, "E", "name", "E");
        Vertex f = graph.addVertex(T.label, "movie", T.id, "F", "name", "F");
        Vertex g = graph.addVertex(T.label, "movie", T.id, "G", "name", "G");
        Vertex h = graph.addVertex(T.label, "movie", T.id, "H", "name", "H");
        Vertex i = graph.addVertex(T.label, "movie", T.id, "I", "name", "I");
        Vertex j = graph.addVertex(T.label, "movie", T.id, "J", "name", "J");

        Vertex k = graph.addVertex(T.label, "person", T.id, "K", "name", "K");
        Vertex l = graph.addVertex(T.label, "person", T.id, "L", "name", "L");
        Vertex m = graph.addVertex(T.label, "person", T.id, "M", "name", "M");

        o.addEdge("follow", a);
        o.addEdge("follow", b);
        o.addEdge("follow", c);
        d.addEdge("follow", o);

        a.addEdge("follow", b);
        a.addEdge("like", e);
        a.addEdge("like", f);

        b.addEdge("like", g);
        b.addEdge("like", h);

        c.addEdge("like", i);
        c.addEdge("like", j);

        e.addEdge("directedBy", k);
        f.addEdge("directedBy", b);
        f.addEdge("directedBy", l);

        g.addEdge("directedBy", m);

        graph.tx().commit();
    }

    public static void loadPersonalRankData(final HugeGraph graph) {
        SchemaManager schema = graph.schema();

        schema.propertyKey("name").asText().ifNotExist().create();

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

        schema.edgeLabel("like")
              .sourceLabel("person")
              .targetLabel("movie")
              .ifNotExist()
              .create();

        graph.tx().open();

        Vertex aPerson = graph.addVertex(T.label, "person", T.id, "A", "name", "A");
        Vertex bPerson = graph.addVertex(T.label, "person", T.id, "B", "name", "B");
        Vertex cPerson = graph.addVertex(T.label, "person", T.id, "C", "name", "C");

        Vertex a = graph.addVertex(T.label, "movie", T.id, "a", "name", "a");
        Vertex b = graph.addVertex(T.label, "movie", T.id, "b", "name", "b");
        Vertex c = graph.addVertex(T.label, "movie", T.id, "c", "name", "c");
        Vertex d = graph.addVertex(T.label, "movie", T.id, "d", "name", "d");

        aPerson.addEdge("like", a);
        aPerson.addEdge("like", c);

        bPerson.addEdge("like", a);
        bPerson.addEdge("like", b);
        bPerson.addEdge("like", c);
        bPerson.addEdge("like", d);

        cPerson.addEdge("like", c);
        cPerson.addEdge("like", d);

        graph.tx().commit();
    }
}
