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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;

public class SchemaCoreTest extends BaseCoreTest{

    // PropertyKey tests
    @Test
    public void testAddPropertyKeyWithValidName() {
        SchemaManager schema = graph().schema();

        // One space and single char
        schema.propertyKey(" s").create();
        schema.propertyKey("s ").create();
        schema.propertyKey(" s ").create();
        schema.propertyKey("s s").create();

        schema.propertyKey(" .").create();
        schema.propertyKey(". ").create();
        schema.propertyKey(" . ").create();
        schema.propertyKey(". .").create();

        schema.propertyKey("~@$%^&*()_+`-={}|[]\"<?;',./\\").create();
        schema.propertyKey("azAZ0123456789").create();

        schema.propertyKey("姓名").create();
        schema.propertyKey(" 姓名").create();
        schema.propertyKey("姓名 ").create();
        schema.propertyKey(" 姓名 ").create();
        schema.propertyKey("姓 名").create();
    }

    @Test
    public void testAddPropertyKeyWithIllegalName() {
        SchemaManager schema = graph().schema();

        // Empty string
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("").create();
        });
        // One space
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey(" ").create();
        });
        // Two spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("  ").create();
        });
        // Multi spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("    ").create();
        });

        // Internal characters
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("#").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey(">").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey(":").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("!").create();
        });
    }

    @Test
    public void testAddPropertyKeyWithoutDataType() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id").create();
        Assert.assertNotNull(id);
        Assert.assertEquals(DataType.TEXT, id.dataType());
    }

    @Test
    public void testAddPropertyKey() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();

        Assert.assertNotNull(schema.getPropertyKey("name"));
        Assert.assertEquals("name", schema.getPropertyKey("name").name());
        Assert.assertEquals(DataType.TEXT,
                            schema.getPropertyKey("name").dataType());
    }

    // VertexLabel tests
    @Test
    public void testAddVertexLabel() {
        initProperties();
        SchemaManager schema = graph().schema();

        VertexLabel person = schema.vertexLabel("person")
                             .properties("name", "age", "city")
                             .primaryKeys("name")
                             .create();

        Assert.assertNotNull(person);
        Assert.assertEquals("person", person.name());
        Assert.assertEquals(3, person.properties().size());
        Assert.assertTrue(person.properties().contains("name"));
        Assert.assertTrue(person.properties().contains("age"));
        Assert.assertTrue(person.properties().contains("city"));
        Assert.assertEquals(1, person.primaryKeys().size());
        Assert.assertTrue(person.primaryKeys().contains("name"));
    }

    @Test
    public void testAddVertexLabelWithIllegalName() {
        SchemaManager schema = graph().schema();

        // Empty string
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("").create();
        });
        // One space
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel(" ").create();
        });
        // Two spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("  ").create();
        });
        // Multi spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("    ").create();
        });
    }

    @Test
    public void testAddVertexLabelWithIdStrategy() {
        initProperties();
        SchemaManager schema = graph().schema();

        VertexLabel person = schema.vertexLabel("person")
                             .properties("name", "age", "city")
                             .create();
        Assert.assertEquals(IdStrategy.AUTOMATIC, person.idStrategy());

        VertexLabel person1 = schema.vertexLabel("person1")
                              .useAutomaticId()
                              .properties("name", "age", "city")
                              .create();
        Assert.assertEquals(IdStrategy.AUTOMATIC, person1.idStrategy());

        VertexLabel person2 = schema.vertexLabel("person2")
                              .useCustomizeId()
                              .properties("name", "age", "city")
                              .create();
        Assert.assertEquals(IdStrategy.CUSTOMIZE, person2.idStrategy());

        VertexLabel person3 = schema.vertexLabel("person3")
                              .properties("name", "age", "city")
                              .primaryKeys("name")
                              .create();
        Assert.assertEquals(IdStrategy.PRIMARY_KEY, person3.idStrategy());

        VertexLabel person4 = schema.vertexLabel("person4")
                              .usePrimaryKeyId()
                              .properties("name", "age", "city")
                              .primaryKeys("name")
                              .create();
        Assert.assertEquals(IdStrategy.PRIMARY_KEY, person4.idStrategy());
    }

    @Test
    public void testAddVertexLabelWithNonPKIdStrategyWithoutProperty() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        VertexLabel person1 = schema.vertexLabel("person1")
                              .useAutomaticId()
                              .create();
        Assert.assertEquals(IdStrategy.AUTOMATIC, person1.idStrategy());
        Assert.assertTrue(person1.properties().isEmpty());

        VertexLabel person2 = schema.vertexLabel("person2")
                              .useCustomizeId()
                              .create();
        Assert.assertEquals(IdStrategy.CUSTOMIZE, person2.idStrategy());
        Assert.assertTrue(person2.properties().isEmpty());
    }

    @Test
    public void testAddVertexLabelWithMultiIdStrategy() {
        initProperties();
        SchemaManager schema = graph().schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .useCustomizeId()
                  .useAutomaticId()
                  .properties("name", "age", "city")
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .useCustomizeId()
                  .usePrimaryKeyId()
                  .properties("name", "age", "city")
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .usePrimaryKeyId()
                  .useAutomaticId()
                  .properties("name", "age", "city")
                  .create();
        });
    }

    @Test
    public void testAddVertexLabelWithoutPKStrategyButCallPrimaryKey() {
        initProperties();
        SchemaManager schema = graph().schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .useCustomizeId()
                  .primaryKeys("name")
                  .properties("name", "age", "city")
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .useCustomizeId()
                  .primaryKeys("name")
                  .properties("name", "age", "city")
                  .create();
        });
    }

    @Test
    public void testAddVertexWithDefaultIdStrategyAndPassedPk() {
        initProperties();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        VertexLabel person = schema.vertexLabel("person")
                             .properties("name", "age")
                             .primaryKeys("name")
                             .create();
        Assert.assertEquals(IdStrategy.PRIMARY_KEY, person.idStrategy());
    }

    @Test
    public void testAddVertexWithDefaultIdStrategyAndNotPassedPk() {
        initProperties();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        VertexLabel person = schema.vertexLabel("person")
                             .properties("name", "age")
                             .create();
        Assert.assertEquals(IdStrategy.AUTOMATIC, person.idStrategy());
    }

    @Test
    public void testAddVertexWithAutomaticIdStrategyButPassedPk() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .useAutomaticId()
                  .properties("name", "age")
                  .primaryKeys("name")
                  .create();
        });
    }

    @Test
    public void testAddVertexWithAutomaticIdStrategyAndNotPassedPk() {
        initProperties();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        VertexLabel person = schema.vertexLabel("person")
                             .useAutomaticId()
                             .properties("name", "age")
                             .create();
        Assert.assertEquals(IdStrategy.AUTOMATIC, person.idStrategy());
    }

    @Test
    public void testAddVertexWithCustomizeIdStrategyButPassedPk() {
        initProperties();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .useCustomizeId()
                  .properties("name", "age")
                  .primaryKeys("name")
                  .create();
        });
    }

    @Test
    public void testAddVertexWithCustomizeIdStrategyAndNotPassedPk() {
        initProperties();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        VertexLabel person = schema.vertexLabel("person")
                             .useCustomizeId()
                             .properties("name", "age")
                             .create();
        Assert.assertEquals(IdStrategy.CUSTOMIZE, person.idStrategy());
    }

    @Test
    public void testAddVertexWithPrimaryKeyIdStrategyAndPassedPk() {
        initProperties();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        VertexLabel person = schema.vertexLabel("person")
                             .usePrimaryKeyId()
                             .properties("name", "age")
                             .primaryKeys("name")
                             .create();
        Assert.assertEquals(IdStrategy.PRIMARY_KEY, person.idStrategy());
    }

    @Test
    public void testAddVertexWithPrimaryKeyIdStrategyButNotPassedPk() {
        initProperties();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .usePrimaryKeyId()
                  .properties("name", "age")
                  .create();
        });
    }

    @Test
    public void testAddVertexLabelWith2PrimaryKey() {
        initProperties();
        SchemaManager schema = graph().schema();

        VertexLabel person = schema.vertexLabel("person")
                             .properties("name", "age", "city")
                             .primaryKeys("name", "age")
                             .create();

        Assert.assertNotNull(person);
        Assert.assertEquals("person", person.name());
        Assert.assertEquals(3, person.properties().size());
        Assert.assertTrue(person.properties().contains("name"));
        Assert.assertTrue(person.properties().contains("age"));
        Assert.assertTrue(person.properties().contains("city"));
        Assert.assertEquals(2, person.primaryKeys().size());
        Assert.assertTrue(person.primaryKeys().contains("name"));
        Assert.assertTrue(person.primaryKeys().contains("age"));
    }

    @Test
    public void testAddVertexLabelWithoutPrimaryKey() {
        initProperties();
        SchemaManager schema = graph().schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .usePrimaryKeyId()
                  .properties("name", "age", "city")
                  .create();
        });
    }

    @Test
    public void testAddVertexLabelWithoutPropertyKey() {
        initProperties();
        SchemaManager schema = graph().schema();

        VertexLabel person = schema.vertexLabel("person").create();

        Assert.assertTrue(person.properties().isEmpty());
    }

    @Test
    public void testAddVertexLabelWithPrimaryKeyNotInProperty() {
        initProperties();
        SchemaManager schema = graph().schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .properties("name", "age", "city")
                  .primaryKeys("name", "sex")
                  .ifNotExist()
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .primaryKeys("name", "sex")
                  .ifNotExist()
                  .create();
        });
    }

    @Test
    public void testAddVertexLabelWithNotExistProperty() {
        initProperties();
        SchemaManager schema = graph().schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person").properties("sex").create();
        });
    }

    @Test
    public void testAddVertexLabelNewVertexWithUndefinedProperty() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "person", "name", "Baby",
                              "city", "Hongkong", "age", 3, "sex", "male");
        });
    }

    @Test
    public void testAddVertexLabelNewVertexWithPropertyAbsent() {
        initProperties();
        SchemaManager schema = graph().schema();
        VertexLabel person = schema.vertexLabel("person")
                             .properties("name", "age", "city")
                             .primaryKeys("name")
                             .nullableKeys("age")
                             .create();

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong");

        Assert.assertNotNull(person);
        Assert.assertEquals("person", person.name());
        Assert.assertEquals(3, person.properties().size());
        Assert.assertTrue(person.properties().contains("name"));
        Assert.assertTrue(person.properties().contains("age"));
        Assert.assertTrue(person.properties().contains("city"));
        Assert.assertEquals(1, person.primaryKeys().size());
        Assert.assertTrue(person.primaryKeys().contains("name"));
    }

    @Test
    public void testAddVertexLabelNewVertexWithUnmatchPropertyType() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "person", "name", "Baby",
                              "city", 2, "age", 3);
        });

    }

    @Test
    public void testAddVertexLabelWithNullableKeys() {
        initProperties();
        SchemaManager schema = graph().schema();

        VertexLabel person = schema.vertexLabel("person")
                             .properties("name", "age", "city")
                             .primaryKeys("name")
                             .nullableKeys("city")
                             .create();

        Assert.assertNotNull(person);
        Assert.assertEquals("person", person.name());
        Assert.assertEquals(3, person.properties().size());
        Assert.assertTrue(person.properties().contains("name"));
        Assert.assertTrue(person.properties().contains("age"));
        Assert.assertTrue(person.properties().contains("city"));
        Assert.assertEquals(1, person.primaryKeys().size());
        Assert.assertTrue(person.primaryKeys().contains("name"));
        Assert.assertEquals(1, person.nullableKeys().size());
        Assert.assertTrue(person.nullableKeys().contains("city"));
    }

    @Test
    public void testAddVertexLabelNewVertexWithNullableKeysAbsent() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        Vertex vertex = graph().addVertex(T.label, "person", "name", "Baby",
                                          "age", 3);
        Assert.assertEquals("Baby", vertex.value("name"));
        Assert.assertEquals(3, vertex.property("age").value());
    }

    @Test
    public void testAddVertexLabelNewVertexWithNotNullableKeysAbsent() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'age'
            graph().addVertex(T.label, "person", "name", "Baby",
                              "city", "Beijing");
        });
    }

    // Edgelabel tests
    @Test
    public void testAddEdgeLabel() {
        initProperties();
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
        initProperties();
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
    }

    @Test
    public void testAddEdgeLabelWithoutFrequency() {
        initProperties();
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
        initProperties();
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
        initProperties();
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
        initProperties();
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
        initProperties();
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
    public void testAddEdgeLabelMultipleWithoutSortKey() {
        initProperties();
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
        initProperties();
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
        initProperties();
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
    public void testAddEdgeLabelNewEdgeWithNullableKeysAbsent() {
        initProperties();
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

        Edge edge = baby.addEdge("look", java, "time", "2017-09-09");
        Assert.assertEquals("look", edge.label());
        Assert.assertEquals("2017-09-09", edge.value("time"));
    }

    @Test
    public void testAddEdgeLabelNewVertexWithNotNullableKeysAbsent() {
        initProperties();
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
    }

    @Test
    public void testRemoveNotExistPropertyKey() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("not-exist-pk").remove();
    }

    @Test
    public void testRemoveVertexLabel() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        Assert.assertNotNull(schema.getVertexLabel("person"));

        schema.vertexLabel("person").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getVertexLabel("person");
        });
    }

    @Test
    public void testRemoveNotExistVertexLabel() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("not-exist-vl").remove();
    }

    @Test
    public void testRemoveVertexLabelWithVertex() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        graph().addVertex(T.label, "person", "name", "marko", "age", 22);
        graph().addVertex(T.label, "person", "name", "jerry", "age", 5);
        graph().addVertex(T.label, "person", "name", "tom", "age", 8);

        List<Vertex> vertex = graph().traversal().V().hasLabel("person")
                                     .toList();
        Assert.assertNotNull(vertex);
        Assert.assertEquals(3, vertex.size());

        schema.vertexLabel("person").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getVertexLabel("person");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().V().hasLabel("person").toList();
        });
    }

    @Test
    public void testRemoveVertexLabelWithVertexAndSearchIndex() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.indexLabel("personByAge").onV("person").by("age").search()
              .create();

        graph().addVertex(T.label, "person", "name", "marko", "age", 22);
        graph().addVertex(T.label, "person", "name", "jerry", "age", 5);
        graph().addVertex(T.label, "person", "name", "tom", "age", 8);

        List<Vertex> vertex = graph().traversal().V().hasLabel("person")
                              .has("age", P.inside(4, 10)).toList();
        Assert.assertNotNull(vertex);
        Assert.assertEquals(2, vertex.size());

        schema.vertexLabel("person").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getVertexLabel("person");
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByAge");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().V().hasLabel("person").toList();
        });
    }

    @Test
    public void testRemoveVertexLabelWithVertexAndSecondaryIndex() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("age")
              .create();

        schema.indexLabel("personByCity").onV("person").by("city").secondary()
              .create();

        graph().addVertex(T.label, "person", "name", "marko",
                          "city", "Beijing");
        graph().addVertex(T.label, "person", "name", "jerry",
                          "city", "Beijing");
        graph().addVertex(T.label, "person", "name", "tom",
                          "city", "HongKong");

        List<Vertex> vertex = graph().traversal().V().hasLabel("person")
                              .has("city", "Beijing").toList();
        Assert.assertNotNull(vertex);
        Assert.assertEquals(2, vertex.size());

        schema.vertexLabel("person").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getVertexLabel("person");
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().V().hasLabel("person").toList();
        });
    }

    @Test
    public void testRemoveEdgeLabel() {
        initProperties();
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
        initProperties();
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

        List<Edge> edge = graph().traversal().E().hasLabel("write").toList();
        Assert.assertNotNull(edge);
        Assert.assertEquals(2, edge.size());

        schema.edgeLabel("write").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getEdgeLabel("write");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().E().hasLabel("write").toList();
        });
    }

    @Test
    public void testRemoveEdgeLabelWithEdgeWithSearchIndex() {
        initProperties();
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

        List<Edge> edge = graph().traversal().E().hasLabel("write")
                          .has("weight", 0.5)
                          .toList();
        Assert.assertNotNull(edge);
        Assert.assertEquals(1, edge.size());

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
    public void testRemoveEdgeLabelWithEdgeWithSecondaryIndex() {
        initProperties();
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

        List<Edge> edge = graph().traversal().E().hasLabel("write")
                          .has("time", "2016-12-12")
                          .toList();
        Assert.assertNotNull(edge);
        Assert.assertEquals(1, edge.size());

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

    @Test
    public void testRemoveVertexLabelUsedByEdgeLabel() {
        initProperties();
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

        marko.addEdge("write", java, "time", "2016-12-12", "weight", 0.3);

        Assert.assertThrows(HugeException.class, () -> {
            schema.vertexLabel("person").remove();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.vertexLabel("book").remove();
        });
    }

    // IndexLabel tests
    @Test
    public void testAddIndexLabelOfVertex() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").search()
              .by("age").create();

        VertexLabel person = schema.getVertexLabel("person");
        IndexLabel personByCity = schema.getIndexLabel("personByCity");
        IndexLabel personByAge = schema.getIndexLabel("personByAge");

        Assert.assertNotNull(personByCity);
        Assert.assertNotNull(personByAge);
        Assert.assertEquals(2, person.indexNames().size());
        Assert.assertTrue(person.indexNames().contains("personByCity"));
        Assert.assertTrue(person.indexNames().contains("personByAge"));
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByCity.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByAge.baseType());
        Assert.assertEquals("person", personByCity.baseValue());
        Assert.assertEquals("person", personByAge.baseValue());
        Assert.assertEquals(IndexType.SECONDARY, personByCity.indexType());
        Assert.assertEquals(IndexType.SEARCH, personByAge.indexType());
    }

    @Test
    public void testAddIndexLabelWithIllegalName() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        // Empty string
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("").onV("person").by("name").create();
        });
        // One space
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel(" ").onV("person").by("name").create();
        });
        // Two spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("  ").onV("person").by("name").create();
        });
        // Multi spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("    ").onV("person").by("name").create();
        });
    }

    @Test
    public void testAddIndexLabelOfEdge() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime()
              .link("author", "book")
              .properties("contribution")
              .create();

        schema.indexLabel("authoredByContri").onE("authored").secondary()
              .by("contribution").create();

        EdgeLabel authored = schema.getEdgeLabel("authored");
        IndexLabel authoredByContri = schema.getIndexLabel("authoredByContri");

        Assert.assertNotNull(authoredByContri);
        Assert.assertEquals(1, authored.indexNames().size());
        Assert.assertTrue(authored.indexNames().contains("authoredByContri"));
        Assert.assertEquals(HugeType.EDGE_LABEL, authoredByContri.baseType());
        Assert.assertEquals("authored", authoredByContri.baseValue());
        Assert.assertEquals(IndexType.SECONDARY, authoredByContri.indexType());
    }

    @Test
    public void testAddIndexLabelOfVertexWithVertexExist() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);

        Assert.assertThrows(BackendException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("city", "Hongkong").next();
        });

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();

        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);

        Assert.assertThrows(BackendException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("age", P.inside(2, 4)).next();
        });
        schema.indexLabel("personByAge").onV("person").search()
              .by("age").create();

        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);
    }

    @Test
    public void testAddIndexLabelOfEdgeWithEdgeExist() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime().link("author", "book")
              .properties("contribution").create();

        Vertex james = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "James Gosling");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "java-1");

        james.addEdge("authored", java1,"contribution", "test");
        Assert.assertThrows(BackendException.class, () -> {
            graph().traversal().E().hasLabel("authored")
                   .has("contribution", "test").next();
        });

        schema.indexLabel("authoredByContri").onE("authored")
              .secondary().by("contribution").create();

        Edge edge = graph().traversal().E().hasLabel("authored")
                           .has("contribution", "test").next();
        Assert.assertNotNull(edge);
    }

    @Test
    public void testAddIndexlabelOnUndefinedSchemaLabel() {
        initProperties();
        SchemaManager schema = graph().schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByName").onV("undefined-vertex-label")
                  .by("name").secondary().create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authoredByContri").onE("undefined-edge-label")
                  .by("contribution").secondary().create();
        });
    }

    @Test
    public void testAddIndexlabelByUndefinedProperty() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime().link("author", "book")
              .properties("contribution").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByData").onV("author")
                  .by("undefined-property").secondary().create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authoredByData").onE("authored")
                  .by("undefined-property").secondary().create();
        });
    }

    @Test
    public void testAddIndexlabelByUnbelongedProperty() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime().link("author", "book")
              .properties("contribution").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("bookById").onV("book")
                  .by("id").secondary().create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authoredByName").onE("authored")
                  .by("name").secondary().create();
        });
    }

    @Test
    public void testRemoveNotExistIndexLabel() {
        SchemaManager schema = graph().schema();
        schema.indexLabel("not-exist-il").remove();
    }

    @Test
    public void testRemoveIndexLabelOfVertex() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").search()
              .by("age").create();
        VertexLabel person = schema.getVertexLabel("person");

        Assert.assertEquals(2, person.indexNames().size());
        Assert.assertTrue(person.indexNames().contains("personByCity"));
        Assert.assertTrue(person.indexNames().contains("personByAge"));

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);

        schema.indexLabel("personByCity").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });

        Assert.assertEquals(1, person.indexNames().size());
        Assert.assertTrue(!person.indexNames().contains("personByCity"));
        Assert.assertTrue(person.indexNames().contains("personByAge"));

        Assert.assertThrows(BackendException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("city", "Hongkong").next();
        });
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);

        schema.indexLabel("personByAge").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByAge");
        });

        person = schema.getVertexLabel("person");
        Assert.assertEquals(0, person.indexNames().size());

        Assert.assertThrows(BackendException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("age", P.inside(2, 4)).next();
        });
    }

    @Test
    public void testRemoveIndexLabelOfEdge() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime()
              .link("author", "book")
              .properties("contribution")
              .create();

        Vertex james = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "James Gosling");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "java-1");

        schema.indexLabel("authoredByContri").onE("authored").secondary()
              .by("contribution").create();

        EdgeLabel authored = schema.getEdgeLabel("authored");

        Assert.assertEquals(1, authored.indexNames().size());
        Assert.assertTrue(authored.indexNames().contains("authoredByContri"));

        james.addEdge("authored", java1,"contribution", "test");

        Edge edge = graph().traversal().E().hasLabel("authored")
                           .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.indexLabel("authoredByContri").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("authoredByContri");
        });

        Assert.assertEquals(0, authored.indexNames().size());

        Assert.assertThrows(BackendException.class, () -> {
            graph().traversal().E().hasLabel("authored")
                   .has("contribution", "test").next();
        });
    }

    @Test
    public void testRebuildIndexLabelOfVertex() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").search()
              .by("age").create();
        VertexLabel person = schema.getVertexLabel("person");
        Assert.assertEquals(2, person.indexNames().size());
        Assert.assertTrue(person.indexNames().contains("personByCity"));
        Assert.assertTrue(person.indexNames().contains("personByAge"));

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);

        schema.indexLabel("personByCity").rebuild();
        vertex = graph().traversal().V().hasLabel("person")
                        .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);

        schema.indexLabel("personByAge").rebuild();
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);
    }

    @Test
    public void testRebuildIndexLabelOfVertexLabel() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").search()
              .by("age").create();

        VertexLabel person = schema.getVertexLabel("person");
        Assert.assertEquals(2, person.indexNames().size());
        Assert.assertTrue(person.indexNames().contains("personByCity"));
        Assert.assertTrue(person.indexNames().contains("personByAge"));

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);

        schema.vertexLabel("person").rebuildIndex();
        vertex = graph().traversal().V().hasLabel("person")
                        .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);
    }

    @Test
    public void testRebuildIndexLabelOfEdgeLabel() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime()
              .link("author", "book")
              .properties("contribution")
              .create();

        Vertex james = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "James Gosling");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "java-1");

        schema.indexLabel("authoredByContri").onE("authored").secondary()
              .by("contribution").create();

        EdgeLabel authored = schema.getEdgeLabel("authored");

        Assert.assertEquals(1, authored.indexNames().size());
        Assert.assertTrue(authored.indexNames().contains("authoredByContri"));

        james.addEdge("authored", java1,"contribution", "test");

        Edge edge = graph().traversal().E().hasLabel("authored")
                           .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.indexLabel("authoredByContri").rebuild();
        Assert.assertEquals(1, authored.indexNames().size());
        Assert.assertTrue(authored.indexNames().contains("authoredByContri"));

        edge = graph().traversal().E().hasLabel("authored")
                      .has("contribution", "test").next();
        Assert.assertNotNull(edge);
    }

    @Test
    public void testRebuildIndexLabelOfEdge() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime()
              .link("author", "book")
              .properties("contribution")
              .create();

        Vertex james = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "James Gosling");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "java-1");

        schema.indexLabel("authoredByContri").onE("authored")
              .secondary().by("contribution").create();

        EdgeLabel authored = schema.getEdgeLabel("authored");

        Assert.assertEquals(1, authored.indexNames().size());
        Assert.assertTrue(authored.indexNames().contains("authoredByContri"));

        james.addEdge("authored", java1,"contribution", "test");

        Edge edge = graph().traversal().E().hasLabel("authored")
                           .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.edgeLabel("authored").rebuildIndex();
        Assert.assertEquals(1, authored.indexNames().size());
        Assert.assertTrue(authored.indexNames().contains("authoredByContri"));
        edge = graph().traversal().E().hasLabel("authored")
                      .has("contribution", "test").next();
        Assert.assertNotNull(edge);
    }

    /**
     * Utils method to init some properties
     */
    private void initProperties() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("contribution").asText().create();
        schema.propertyKey("weight").asDouble().create();
    }
}
