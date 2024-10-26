/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.SimpleClusterTest;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.GremlinManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Path;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.gremlin.Result;
import org.apache.hugegraph.structure.gremlin.ResultSet;
import org.junit.Assert;
import org.junit.Test;

public class SimpleClusterDeployTest extends BaseSimpleTest {

    @Test
    public void testPDNodesDeployment() {
        try {
            List<String> addrs = env.getPDGrpcAddrs();
            for (String addr : addrs) {
                PDConfig pdConfig = PDConfig.of(addr);
                pdClient = PDClient.create(pdConfig);
                pdClient.dbCompaction();
            }
            assert true;
        } catch (PDException pdException) {
            assert false;
        }
    }

    @Test
    public void testStoreNodesDeployment() throws IOException {
        List<String> addrs = env.getStoreRestAddrs();
        for (String addr : addrs) {
            String[] cmds = {"curl", addr};
            // TODO: what's the purpose of this?
            StringBuilder sb = new StringBuilder();
            for (String cmd : cmds) {
                sb.append(cmd).append(" ");
            }
            String responseMsg = execCmd(cmds);
            Assert.assertTrue(responseMsg.startsWith("{"));
        }
    }

    @Test
    public void testServerNodesDeployment() {
        List<String> addrs = env.getServerRestAddrs();
        for (String addr : addrs) {
            hugeClient = HugeClient.builder("http://" + addr, "hugegraph").build();
            SchemaManager schema = hugeClient.schema();

            schema.propertyKey("name").asText().ifNotExist().create();
            schema.propertyKey("age").asInt().ifNotExist().create();
            schema.propertyKey("city").asText().ifNotExist().create();
            schema.propertyKey("weight").asDouble().ifNotExist().create();
            schema.propertyKey("lang").asText().ifNotExist().create();
            schema.propertyKey("date").asDate().ifNotExist().create();
            schema.propertyKey("price").asInt().ifNotExist().create();

            schema.vertexLabel("person")
                  .properties("name", "age", "city")
                  .primaryKeys("name")
                  .ifNotExist()
                  .create();

            schema.vertexLabel("software")
                  .properties("name", "lang", "price")
                  .primaryKeys("name")
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
                  .ifNotExist()
                  .create();

            schema.indexLabel("softwareByPrice")
                  .onV("software")
                  .by("price")
                  .range()
                  .ifNotExist()
                  .create();

            schema.edgeLabel("knows")
                  .sourceLabel("person")
                  .targetLabel("person")
                  .properties("date", "weight")
                  .ifNotExist()
                  .create();

            schema.edgeLabel("created")
                  .sourceLabel("person").targetLabel("software")
                  .properties("date", "weight")
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
                  .range()
                  .ifNotExist()
                  .create();

            schema.indexLabel("knowsByWeight")
                  .onE("knows")
                  .by("weight")
                  .range()
                  .ifNotExist()
                  .create();

            GraphManager graph = hugeClient.graph();
            Vertex marko = graph.addVertex(T.LABEL, "person", "name", "marko",
                                           "age", 29, "city", "Beijing");
            Vertex vadas = graph.addVertex(T.LABEL, "person", "name", "vadas",
                                           "age", 27, "city", "Hongkong");
            Vertex lop = graph.addVertex(T.LABEL, "software", "name", "lop",
                                         "lang", "java", "price", 328);
            Vertex josh = graph.addVertex(T.LABEL, "person", "name", "josh",
                                          "age", 32, "city", "Beijing");
            Vertex ripple = graph.addVertex(T.LABEL, "software", "name", "ripple",
                                            "lang", "java", "price", 199);
            Vertex peter = graph.addVertex(T.LABEL, "person", "name", "peter",
                                           "age", 35, "city", "Shanghai");

            marko.addEdge("knows", vadas, "date", "2016-01-10", "weight", 0.5);
            marko.addEdge("knows", josh, "date", "2013-02-20", "weight", 1.0);
            marko.addEdge("created", lop, "date", "2017-12-10", "weight", 0.4);
            josh.addEdge("created", lop, "date", "2009-11-11", "weight", 0.4);
            josh.addEdge("created", ripple, "date", "2017-12-10", "weight", 1.0);
            peter.addEdge("created", lop, "date", "2017-03-24", "weight", 0.2);

            GremlinManager gremlin = hugeClient.gremlin();
            System.out.println("==== Path ====");
            ResultSet resultSet = gremlin.gremlin("g.V().outE().path()").execute();
            Iterator<Result> results = resultSet.iterator();
            results.forEachRemaining(result -> {
                System.out.println(result.getObject().getClass());
                Object object = result.getObject();
                if (object instanceof Vertex) {
                    System.out.println(((Vertex) object).id());
                } else if (object instanceof Edge) {
                    System.out.println(((Edge) object).id());
                } else if (object instanceof Path) {
                    List<Object> elements = ((Path) object).objects();
                    elements.forEach(element -> {
                        System.out.println(element.getClass());
                        System.out.println(element);
                    });
                } else {
                    System.out.println(object);
                }
            });

            hugeClient.close();
        }
    }
}
