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

package org.apache.hugegraph.core.memory;

import java.nio.file.Paths;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.memory.consumer.impl.id.StringIdOffHeap;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MemoryConsumerTest extends MemoryManageTest {

    private static HugeGraph graph;
    private static boolean registered = false;

    public static HugeGraph buildGraph() throws ConfigurationException {
        String projectRoot = Paths.get("").toAbsolutePath().getParent().getParent().toString();
        String CONF = projectRoot + "/hugegraph-commons/hugegraph-common/src/test/java/org/apache" +
                      "/hugegraph/unit/config/test.conf";
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        FileHandler fileHandler = new FileHandler(configuration);
        fileHandler.load(CONF);
        HugeConfig config = new HugeConfig(configuration);
        return new StandardHugeGraph(config);
    }

    @BeforeClass
    public static void setup() throws ConfigurationException {
        graph = buildGraph();
        graph.clearBackend();
        graph.initBackend();
        graph.serverStarted(GlobalMasterInfo.master("server-test"));
        if (registered) {
            return;
        }
        RegisterUtil.registerBackends();
        registered = true;

        SchemaManager schema = graph.schema();
        schema.propertyKey("name").asText().create();
        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();
    }

    @AfterClass
    public static void clear() throws Exception {
        if (graph == null) {
            return;
        }

        try {
            graph.clearBackend();
        } finally {
            graph.close();
            graph = null;
        }
    }

    @Test
    public void testId() {
        Id stringIdOffHeap = new StringIdOffHeap(query1Task1Operator1MemoryPool, "java");
        HugeVertex java = new HugeVertex(graph, stringIdOffHeap, graph.vertexLabel("book"));
        Id edgeLableId = new StringIdOffHeap(query1Task1Operator1MemoryPool, "testEdgeLabel");
        Id subLableId = new StringIdOffHeap(query1Task1Operator1MemoryPool, "testSubLabel");
        // Id id = new EdgeIdOffHeap(java, Directions.OUT, edgeLableId, subLableId, "test", java);
    }

    @Test
    public void testProperty() {

    }
}
