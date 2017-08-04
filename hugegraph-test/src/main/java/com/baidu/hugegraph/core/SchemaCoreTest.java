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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.core;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;

public class SchemaCoreTest extends BaseCoreTest{

    // Propertykey tests
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

    // Vertexlabel tests
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
    public void testAddVertexLabelWithIdStrategy() {
        initProperties();
        SchemaManager schema = graph().schema();

        VertexLabel person = schema.vertexLabel("person")
                             .properties("name", "age", "city")
                             .create();
        Assert.assertEquals(IdStrategy.AUTOMATIC, person.idStrategy());

        VertexLabel person1 = schema.vertexLabel("person1")
                              .idStrategy(IdStrategy.AUTOMATIC)
                              .properties("name", "age", "city")
                              .create();
        Assert.assertEquals(IdStrategy.AUTOMATIC, person1.idStrategy());

        VertexLabel person2 = schema.vertexLabel("person2")
                              .idStrategy(IdStrategy.CUSTOMIZE)
                              .properties("name", "age", "city")
                              .create();
        Assert.assertEquals(IdStrategy.CUSTOMIZE, person2.idStrategy());

        VertexLabel person3 = schema.vertexLabel("person3")
                              .properties("name", "age", "city")
                              .primaryKeys("name")
                              .create();
        Assert.assertEquals(IdStrategy.PRIMARY_KEY, person3.idStrategy());

        VertexLabel person4 = schema.vertexLabel("person4")
                              .idStrategy(IdStrategy.PRIMARY_KEY)
                              .properties("name", "age", "city")
                              .primaryKeys("name")
                              .create();
        Assert.assertEquals(IdStrategy.PRIMARY_KEY, person4.idStrategy());
    }

    @Test
    public void testAddVertexLabelWithMultiIdStrategy() {
        initProperties();
        SchemaManager schema = graph().schema();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .idStrategy(IdStrategy.CUSTOMIZE)
                  .idStrategy(IdStrategy.AUTOMATIC)
                  .properties("name", "age", "city")
                  .create();
        });

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .idStrategy(IdStrategy.CUSTOMIZE)
                  .idStrategy(IdStrategy.PRIMARY_KEY)
                  .properties("name", "age", "city")
                  .create();
        });

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .idStrategy(IdStrategy.PRIMARY_KEY)
                  .idStrategy(IdStrategy.AUTOMATIC)
                  .properties("name", "age", "city")
                  .create();
        });
    }

    @Test
    public void testAddVertexLabelWithoutPKStrategyButCallPrimaryKey() {
        initProperties();
        SchemaManager schema = graph().schema();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .idStrategy(IdStrategy.CUSTOMIZE)
                  .primaryKeys("name")
                  .properties("name", "age", "city")
                  .create();
        });

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .idStrategy(IdStrategy.AUTOMATIC)
                  .primaryKeys("name")
                  .properties("name", "age", "city")
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

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person")
                  .idStrategy(IdStrategy.PRIMARY_KEY)
                  .properties("name", "age", "city")
                  .create();
        });
    }

    @Test
    public void testAddVertexLabelWithoutPropertyKey() {
        initProperties();
        SchemaManager schema = graph().schema();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.vertexLabel("person").create();
        });
    }

    @Test
    public void testAddVertexLabelWithNotExistProperty() {
        initProperties();
        SchemaManager schema = graph().schema();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
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

        Utils.assertThrows(IllegalArgumentException.class, () -> {
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

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "person", "name", "Baby",
                    "city", 2, "age", 3);
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

        Utils.assertThrows(IllegalArgumentException.class, () -> {
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

        Utils.assertThrows(IllegalArgumentException.class, () -> {
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

        Utils.assertThrows(IllegalArgumentException.class, () -> {
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

        Utils.assertThrows(IllegalArgumentException.class, () -> {
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

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").multiTimes().properties("date")
                    .link("person", "book")
                    .sortKeys("time")
                    .create();
        });
    }

    // Indexlabel tests
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
        VertexLabel person = schema.vertexLabel("person")
                                   .properties("name", "age", "city")
                                   .primaryKeys("name")
                                   .create();

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);

        Utils.assertThrows(BackendException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("city", "Hongkong").next();
        });

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();

        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);

        Utils.assertThrows(BackendException.class, () -> {
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
        EdgeLabel authored = schema.edgeLabel("authored").singleTime()
                                   .link("author", "book")
                                   .properties("contribution")
                                   .create();

        Vertex james = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "James Gosling");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "java-1");

        james.addEdge("authored", java1,"contribution", "test");
        Utils.assertThrows(BackendException.class, () -> {
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
    public void testRemoveIndexLabelOfVertex() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        IndexLabel personByCity = schema.indexLabel("personByCity")
                                        .onV("person").secondary().by("city")
                                        .create();
        IndexLabel personByAge = schema.indexLabel("personByAge")
                                       .onV("person").search().by("age")
                                       .create();
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

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.getIndexLabel("personByCity");
        });

        Assert.assertEquals(1, person.indexNames().size());
        Assert.assertTrue(!person.indexNames().contains("personByCity"));
        Assert.assertTrue(person.indexNames().contains("personByAge"));

        Utils.assertThrows(BackendException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("city", "Hongkong").next();
        });
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);

        schema.indexLabel("personByAge").remove();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.getIndexLabel("personByAge");
        });

        person = schema.getVertexLabel("person");
        Assert.assertEquals(0, person.indexNames().size());

        Utils.assertThrows(BackendException.class, () -> {
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

        IndexLabel authoredByContri = schema.indexLabel("authoredByContri")
                                            .onE("authored").secondary()
                                            .by("contribution").create();

        EdgeLabel authored = schema.getEdgeLabel("authored");

        Assert.assertEquals(1, authored.indexNames().size());
        Assert.assertTrue(authored.indexNames().contains("authoredByContri"));

        james.addEdge("authored", java1,"contribution", "test");

        Edge edge = graph().traversal().E().hasLabel("authored")
                           .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.indexLabel("authoredByContri").remove();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.getIndexLabel("authoredByContri");
        });

        Assert.assertEquals(0, authored.indexNames().size());

        Utils.assertThrows(BackendException.class, () -> {
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
        IndexLabel personByCity = schema.indexLabel("personByCity")
                                        .onV("person").secondary()
                                        .by("city").create();
        IndexLabel personByAge = schema.indexLabel("personByAge")
                                       .onV("person").search().by("age")
                                       .create();
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

        IndexLabel authoredByContri = schema.indexLabel("authoredByContri")
                                            .onE("authored").secondary()
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


    // utils
    private void initProperties() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("contribution").asText().create();
    }
}
