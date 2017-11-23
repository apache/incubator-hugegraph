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

import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.define.Frequency;

public class EdgeLabelCoreTest extends SchemaCoreTest {

    @Test
    public void testAddEdgeLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look").multiTimes()
                         .properties("time")
                         .link("person", "book")
                         .sortKeys("time")
                         .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        Assert.assertTrue(look.sourceLabel().equals("person"));
        Assert.assertTrue(look.targetLabel().equals("book"));
        Assert.assertEquals(1, look.properties().size());
        Assert.assertTrue(look.properties().contains("time"));
        Assert.assertEquals(1, look.sortKeys().size());
        Assert.assertTrue(look.sortKeys().contains("time"));
        Assert.assertEquals(Frequency.MULTIPLE, look.frequency());
    }

    @Test
    public void testAddEdgeLabelWithIllegalName() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();

        // Empty string
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("").link("person", "author").create();
        });
        // One space
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel(" ").link("person", "author").create();
        });
        // Two spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("  ").link("person", "author").create();
        });
        // Multi spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("    ").link("person", "author").create();
        });
        // Start with '~'
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("~").link("person", "author").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("~ ").link("person", "author").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("~x").link("person", "author").create();
        });
    }

    @Test
    public void testAddEdgeLabelWithoutFrequency() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look").properties("time")
                         .link("person", "book")
                         .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        Assert.assertTrue(look.sourceLabel().equals("person"));
        Assert.assertTrue(look.targetLabel().equals("book"));
        Assert.assertEquals(1, look.properties().size());
        Assert.assertTrue(look.properties().contains("time"));
        Assert.assertEquals(0, look.sortKeys().size());
        Assert.assertEquals(Frequency.SINGLE, look.frequency());
    }

    @Test
    public void testAddEdgeLabelWithoutProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look").singleTime()
                         .link("person", "book")
                         .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        Assert.assertTrue(look.sourceLabel().equals("person"));
        Assert.assertTrue(look.targetLabel().equals("book"));
        Assert.assertEquals(0, look.properties().size());
        Assert.assertEquals(0, look.sortKeys().size());
        Assert.assertEquals(Frequency.SINGLE, look.frequency());
    }

    @Test
    public void testAddEdgeLabelWithoutLink() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").multiTimes()
                    .properties("time")
                    .sortKeys("time")
                    .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNotExistProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").properties("date")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNotExistVertexLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").multiTimes().properties("time")
                  .link("reviewer", "book")
                  .sortKeys("time")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithSortKeyAssignedMultiTimes() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("date")
                  .link("person", "book")
                  .sortKeys("date")
                  .sortKeys("date")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithSortKeyContainSameProp() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("date")
                  .link("person", "book")
                  .sortKeys("date", "date")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelMultipleWithoutSortKey() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").multiTimes().properties("date")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelSortKeyNotInProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").multiTimes().properties("date")
                  .link("person", "book")
                  .sortKeys("time")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNullableKeys() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look")
                         .properties("time", "weight")
                         .nullableKeys("weight")
                         .link("person", "book")
                         .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        Assert.assertTrue(look.sourceLabel().equals("person"));
        Assert.assertTrue(look.targetLabel().equals("book"));
        Assert.assertEquals(2, look.properties().size());
        Assert.assertTrue(look.properties().contains("time"));
        Assert.assertTrue(look.properties().contains("weight"));
        Assert.assertEquals(1, look.nullableKeys().size());
        Assert.assertTrue(look.nullableKeys().contains("weight"));
    }

    @Test
    public void testAddEdgeLabelWithUndefinedNullableKeys() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .properties("time", "weight")
                  .nullableKeys("undefined")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNullableKeysNotInProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .properties("time", "weight")
                  .nullableKeys("age")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNullableKeysIntersectSortkeys() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("time", "weight")
                  .sortKeys("time")
                  .nullableKeys("time")
                  .link("person", "book")
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("time", "weight")
                  .sortKeys("time", "weight")
                  .nullableKeys("time")
                  .link("person", "book")
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("time", "weight")
                  .sortKeys("time")
                  .nullableKeys("time", "weight")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAppendEdgeLabelWithUndefinedNullableKeys() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time", "weight")
              .nullableKeys("time")
              .link("person", "book")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").nullableKeys("undefined").append();
        });
    }

    @Test
    public void testAppendEdgeLabelWithNullableKeysInOriginProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time", "weight")
              .link("person", "book")
              .create();

        EdgeLabel look = schema.edgeLabel("look")
                         .nullableKeys("weight")
                         .append();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        Assert.assertTrue(look.sourceLabel().equals("person"));
        Assert.assertTrue(look.targetLabel().equals("book"));
        Assert.assertEquals(2, look.properties().size());
        Assert.assertTrue(look.properties().contains("time"));
        Assert.assertTrue(look.properties().contains("weight"));
        Assert.assertEquals(1, look.nullableKeys().size());
        Assert.assertTrue(look.nullableKeys().contains("weight"));
    }

    @Test
    public void testAppendEdgeLabelWithNullableKeysInAppendProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time")
              .link("person", "book")
              .create();

        EdgeLabel look = schema.edgeLabel("look")
                         .properties("weight")
                         .nullableKeys("weight")
                         .append();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        Assert.assertTrue(look.sourceLabel().equals("person"));
        Assert.assertTrue(look.targetLabel().equals("book"));
        Assert.assertEquals(2, look.properties().size());
        Assert.assertTrue(look.properties().contains("time"));
        Assert.assertTrue(look.properties().contains("weight"));
        Assert.assertEquals(1, look.nullableKeys().size());
        Assert.assertTrue(look.nullableKeys().contains("weight"));
    }

    @Test
    public void testAppendEdgeLabelWithNullableKeysNotInProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time")
              .link("person", "book")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .nullableKeys("contribution")
                  .append();
        });
    }

    @Test
    public void testAppendEdgeLabelNewEdgeWithNullableKeysAbsent() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time", "weight")
              .nullableKeys("weight")
              .link("person", "book")
              .create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book", "id", 123456,
                                        "name", "Java in action");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'time'
            baby.addEdge("look", java, "weight", 0.5);
        });

        schema.edgeLabel("look")
              .nullableKeys("time")
              .append();
        // Absent 'time'
        Edge edge = baby.addEdge("look", java, "weight", 0.5);
        Assert.assertEquals("look", edge.label());
        Assert.assertEquals(0.5, edge.property("weight").value());
    }

    @Test
    public void testAppendEdgeLabelNewEdgeWithNonNullKeysAbsent() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time", "weight")
              .link("person", "book")
              .create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book", "id", 123456,
                                        "name", "Java in action");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'time'
            baby.addEdge("look", java, "weight", 0.5);
        });

        schema.edgeLabel("look")
              .nullableKeys("weight")
              .append();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'time'
            baby.addEdge("look", java, "weight", 0.5);
        });
    }

    @Test
    public void testAppendEdgeLabelWithNonNullProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time")
              .link("person", "book")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .properties("weight")
                  .append();
        });

    }

    @Test
    public void testRemoveEdgeLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        schema.vertexLabel("book")
              .properties("name", "contribution")
              .primaryKeys("name")
              .create();

        schema.edgeLabel("look").link("person", "book")
              .properties("time", "city")
              .create();

        Assert.assertNotNull(schema.getEdgeLabel("look"));

        schema.edgeLabel("look").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getEdgeLabel("look");
        });
    }

    @Test
    public void testRemoveNotExistEdgeLabel() {
        SchemaManager schema = graph().schema();
        schema.edgeLabel("not-exist-el").remove();
    }

    @Test
    public void testRemoveEdgeLabelWithEdge() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        schema.edgeLabel("write").link("person", "book")
              .properties("time", "weight")
              .create();

        Vertex marko = graph().addVertex(T.label, "person", "name", "marko",
                                         "age", 22);
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "java in action");
        Vertex hadoop = graph().addVertex(T.label, "book",
                                          "name", "hadoop mapreduce");

        marko.addEdge("write", java, "time", "2016-12-12",
                      "weight", 0.3);
        marko.addEdge("write", hadoop, "time", "2014-2-28",
                      "weight", 0.5);

        graph().tx().commit();

        List<Edge> edges = graph().traversal().E().hasLabel("write").toList();
        Assert.assertNotNull(edges);
        Assert.assertEquals(2, edges.size());

        schema.edgeLabel("write").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getEdgeLabel("write");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().E().hasLabel("write").toList();
        });
    }

    @Test
    public void testRemoveEdgeLabelWithEdgeAndSearchIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        schema.edgeLabel("write").link("person", "book")
              .properties("time", "weight")
              .create();

        schema.indexLabel("writeByWeight").onE("write").by("weight")
              .search()
              .create();

        Vertex marko = graph().addVertex(T.label, "person", "name", "marko",
                                         "age", 22);
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "java in action");
        Vertex hadoop = graph().addVertex(T.label, "book",
                                          "name", "hadoop mapreduce");

        marko.addEdge("write", java, "time", "2016-12-12",
                      "weight", 0.3);
        marko.addEdge("write", hadoop, "time", "2014-2-28",
                      "weight", 0.5);

        List<Edge> edges = graph().traversal().E().hasLabel("write")
                           .has("weight", 0.5).toList();
        Assert.assertNotNull(edges);
        Assert.assertEquals(1, edges.size());

        schema.edgeLabel("write").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getEdgeLabel("write");
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("writeByWeight");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().E().hasLabel("write").toList();
        });
    }

    @Test
    public void testRemoveEdgeLabelWithEdgeAndSecondaryIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        schema.edgeLabel("write").link("person", "book")
              .properties("time", "weight")
              .create();

        schema.indexLabel("writeByTime").onE("write").by("time")
              .secondary()
              .create();

        Vertex marko = graph().addVertex(T.label, "person", "name", "marko",
                                         "age", 22);
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "java in action");
        Vertex hadoop = graph().addVertex(T.label, "book",
                                          "name", "hadoop mapreduce");

        marko.addEdge("write", java, "time", "2016-12-12", "weight", 0.3);
        marko.addEdge("write", hadoop, "time", "2014-2-28", "weight", 0.5);

        List<Edge> edges = graph().traversal().E().hasLabel("write")
                           .has("time", "2016-12-12").toList();
        Assert.assertNotNull(edges);
        Assert.assertEquals(1, edges.size());

        schema.edgeLabel("write").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getEdgeLabel("write");
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("writeByTime");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().E().hasLabel("write").toList();
        });
    }
}
