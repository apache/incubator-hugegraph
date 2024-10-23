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

package org.apache.hugegraph.memory.consumer.impl;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.fury.format.encoder.Encoders;
import org.apache.fury.format.encoder.RowEncoder;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.memory.allocator.Bar;
import org.apache.hugegraph.memory.consumer.MemoryConsumer;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;

public class EdgeFuryVersion extends HugeEdge implements MemoryConsumer {

    private final RowEncoder<EdgeId> idRowEncoder = Encoders.bean(EdgeId.class);
    private final RowEncoder<Bar> barEncoder = Encoders.bean(Bar.class);

    private HugeEdge tmpOnHeapEdge;
    private BinaryRow furyEdge;

    public EdgeFuryVersion(HugeEdge edge) {
        this.tmpOnHeapEdge = edge;
    }

    public void deserialize() {

        RowEncoder<HugeEdge> selfEncoder = Encoders.bean(HugeEdge.class);
        furyEdge = selfEncoder.toRow(tmpOnHeapEdge);
        // let GC collect the tmp onHeap memory edge
        tmpOnHeapEdge = null;
    }

    private Id getId() {
        BinaryRow idStruct = furyEdge.getStruct(0);
        return idRowEncoder.fromRow(idStruct);
    }

    public void printSchema() {
        System.out.println(furyEdge.getSchema());
    }

    public static HugeGraph buildGraph() throws ConfigurationException {
        String PATH =
                "/Users/pengjunzhi/THU/incubator-hugegraph/hugegraph-commons/hugegraph-common/src" +
                "/test/java/org/apache/hugegraph/unit/config/";
        String CONF = PATH + "test.conf";
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        FileHandler fileHandler = new FileHandler(configuration);
        fileHandler.load(CONF);
        HugeConfig config = new HugeConfig(configuration);
        HugeGraph graph = new StandardHugeGraph(config);
        SchemaManager schema = graph.schema();
        schema.propertyKey("name").asText().create();
        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();
        return graph;
    }

    private static Id buildId(HugeGraph graph) {
        HugeVertex java = new HugeVertex(graph, IdGenerator.of("java"),
                                         graph.vertexLabel("book"));
        Id id = new EdgeId(java, Directions.OUT, IdGenerator.of("test"), IdGenerator.of("test"),
                           "test", java);
        return id;
    }

    public static void main(String[] args) throws ConfigurationException {
        HugeGraph graph = buildGraph();
        Id id = buildId(graph);
        HugeEdge testEdge = new HugeEdge(graph, id, EdgeLabel.NONE);
        EdgeFuryVersion edgeFuryVersion = new EdgeFuryVersion(testEdge);
        edgeFuryVersion.deserialize();
        edgeFuryVersion.printSchema();
    }
}
