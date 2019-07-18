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
import org.junit.Assume;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.exception.NoIndexException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;

public class IndexLabelCoreTest extends SchemaCoreTest {

    @Test
    public void testAddIndexLabelOfVertex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("id", "name", "age", "city")
              .primaryKeys("id").create();
        schema.indexLabel("personByName").onV("person").secondary()
              .by("name").create();
        schema.indexLabel("personByCity").onV("person").search()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();

        VertexLabel person = schema.getVertexLabel("person");
        IndexLabel personByName = schema.getIndexLabel("personByName");
        IndexLabel personByCity = schema.getIndexLabel("personByCity");
        IndexLabel personByAge = schema.getIndexLabel("personByAge");

        Assert.assertNotNull(personByName);
        Assert.assertNotNull(personByCity);
        Assert.assertNotNull(personByAge);

        Assert.assertEquals(3, person.indexLabels().size());
        assertContainsIl(person.indexLabels(),
                         "personByName", "personByCity", "personByAge");

        Assert.assertEquals(HugeType.VERTEX_LABEL, personByName.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByCity.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByAge.baseType());

        assertVLEqual("person", personByName.baseValue());
        assertVLEqual("person", personByCity.baseValue());
        assertVLEqual("person", personByAge.baseValue());

        Assert.assertEquals(IndexType.SECONDARY, personByName.indexType());
        Assert.assertEquals(IndexType.SEARCH, personByCity.indexType());
        Assert.assertEquals(IndexType.RANGE4, personByAge.indexType());
    }

    @Test
    public void testAddIndexLabelWithIllegalName() {
        super.initPropertyKeys();
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
        // Start with '~'
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("~").onV("person").by("name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("~ ").onV("person").by("name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("~x").onV("person").by("name").create();
        });
    }

    @Test
    public void testAddIndexLabelOfEdge() {
        super.initPropertyKeys();
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
        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");
        Assert.assertEquals(HugeType.EDGE_LABEL, authoredByContri.baseType());
        assertELEqual("authored", authoredByContri.baseValue());
        Assert.assertEquals(IndexType.SECONDARY, authoredByContri.indexType());
    }

    @Test
    public void testAddIndexLabelOfVertexWithVertexExist() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        graph().tx().commit();

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("city", "Hongkong").next();
        });

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();

        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("age", P.inside(2, 4)).next();
        });
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();

        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);
    }

    @Test
    public void testAddIndexLabelOfEdgeWithEdgeExist() {
        super.initPropertyKeys();
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

        james.addEdge("authored", java1, "contribution", "test");

        graph().tx().commit();

        Assert.assertThrows(NoIndexException.class, () -> {
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
    public void testAddIndexLabelOnUndefinedSchemaLabel() {
        super.initPropertyKeys();
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
    public void testAddIndexLabelByUndefinedProperty() {
        super.initPropertyKeys();
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
    public void testAddIndexLabelByNotBelongedProperty() {
        super.initPropertyKeys();
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
    public void testAddIndexLabelWithInvalidFields() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("author").properties("id", "name", "age")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime().link("author", "book")
              .properties("contribution", "age", "weight").create();

        // Invalid range-index
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByName").onV("author")
                  .by("name").range().create();
        }, e -> {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                              "Range index can only build on numeric"));
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authoredByAgeAndWeight").onE("authored")
                  .by("age", "weight").range().create();
        }, e -> {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                              "Range index can only build on one field"));
        });

        // Invalid search-index
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByAge").onV("author")
                  .by("age").search().create();
        }, e -> {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                              "Search index can only build on text"));
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByNameAndAge").onV("author")
                  .by("name", "age").search().create();
        }, e -> {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                              "Search index can only build on one field"));
        });
    }

    @Test
    public void testAddIndexLabelWithFieldsAssignedMultiTimes() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByCity").onV("person").secondary()
                  .by("city").by("city").create();
        });
    }

    @Test
    public void testAddIndexLabelWithFieldsContainSameProp() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAgeAndCity").onV("person").secondary()
                  .by("age", "city", "age").create();
        });
    }

    @Test
    public void testAddIndexLabelWithSameFieldsBetweenRangeSecondary() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge2").onV("person").secondary()
                  .by("age").create();
        });
    }

    @Test
    public void testAddIndexLabelWithSameFieldsBetweenSearchSecondary() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByCity").onV("person").search()
              .by("city").create();
        schema.indexLabel("personByCity2").onV("person").secondary()
              .by("city").create();
    }


    @Test
    public void testAddIndexLabelWithSameFieldsAndSameIndexType() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByCity1").onV("person").secondary()
                  .by("city").create();
        });

        schema.indexLabel("personByCitySearch").onV("person").search()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByCitySearch1").onV("person").search()
                  .by("city").create();
        });

        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge1").onV("person").range()
                  .by("age").create();
        });

        schema.indexLabel("personByAgeAndCity").onV("person").secondary()
              .by("age", "city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAgeAndCity1").onV("person").secondary()
                  .by("age", "city").create();
        });
    }

    @Test
    public void testAddIndexLabelRangePrefixOfExistedSecondary() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByAgeAndCity").onV("person").secondary()
              .by("age", "city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
    }

    @Test
    public void testAddIndexLabelSecondaryPrefixOfExistedSecondary() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByAgeAndCity").onV("person").secondary()
              .by("age", "city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge").onV("person").secondary()
                  .by("age").create();
        });
    }

    @Test
    public void testAddIndexLabelSecondaryPrefixWithExistedSecondary() {
        super.initPropertyKeys();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3);
        graph.tx().commit();

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAgeSecondary").onV("person").secondary()
                  .by("age").create();
        });

        List<Vertex> vertices;
        vertices = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V().has("city", "Hongkong")
                          .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());

        schema.indexLabel("personByCityAndAge").onV("person").secondary()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });
        schema.getIndexLabel("personByAge");

        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong")
                        .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testAddIndexLabelSecondaryPrefixWithExistedRange() {
        super.initPropertyKeys();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3);
        graph.tx().commit();

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAgeSecondary").onV("person").secondary()
                  .by("age").create();
        });

        List<Vertex> vertices;
        vertices = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V().has("city", "Hongkong")
                          .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());

        schema.indexLabel("personByAgeAndCity").onV("person").secondary()
              .by("age", "city").create();
        schema.getIndexLabel("personByAge");

        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong")
                        .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testAddIndexLabelOnPrimaryKeyProps() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByName").onV("person").secondary()
                  .by("name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByNameAge").onV("person").secondary()
                  .by("name", "age").create();
        });

        schema.vertexLabel("person1").properties("name", "age", "city")
              .primaryKeys("age").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person1ByAge").onV("person1").secondary()
                  .by("age").create();
        });
        schema.indexLabel("person1ByAge").onV("person").range()
              .by("age").create();

        schema.vertexLabel("student").properties("name", "age", "city")
              .primaryKeys("name", "age").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByNameAge").onV("student").secondary()
                  .by("name", "age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByAgeName").onV("student").secondary()
                  .by("age", "name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByNameAgeCity").onV("student").secondary()
                  .by("name", "age", "city").create();
        });
        schema.indexLabel("studentByName").onV("student").secondary()
              .by("name").create();

        schema.indexLabel("studentByNameAge").onV("student").shard()
              .by("name", "age").create();
        schema.indexLabel("studentByAgeName").onV("student").shard()
              .by("age", "name").create();
        schema.indexLabel("studentByNameAgeCity").onV("student").shard()
              .by("name", "age", "city").create();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("studentByName");
        });
    }

    @Test
    public void testAddShardIndexLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city", "weight")
              .primaryKeys("name").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByName").onV("person").shard()
                  .by("name").create();
        });
        schema.indexLabel("personByCity").onV("person").shard()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").shard()
              .by("age").create();
        schema.indexLabel("personByWeight").onV("person").shard()
              .by("weight").create();
        schema.getIndexLabel("personByCity");
        schema.getIndexLabel("personByAge");
        schema.getIndexLabel("personByWeight");

        schema.indexLabel("personByNameAndAge").onV("person").shard()
              .by("name", "age").create();

        schema.indexLabel("personByCityAndAge").onV("person").shard()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });
        schema.getIndexLabel("personByAge");
        schema.getIndexLabel("personByWeight");

        schema.indexLabel("personByAgeAndCity").onV("person").shard()
              .by("age", "city").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByAge");
        });
        schema.getIndexLabel("personByWeight");

        schema.indexLabel("personByNameAgeCityWeight").onV("person").shard()
              .by("name", "age", "city", "weight").create();
    }

    @Test
    public void testAddIndexLabelWithRepeatIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city", "weight")
              .create();

        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge1").onV("person").range()
                  .by("age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge1").onV("person").secondary()
                  .by("age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge2").onV("person").shard()
                  .by("age").create();
        });

        schema.vertexLabel("person1")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person1ByAge").onV("person1").secondary()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person1ByAge1").onV("person1").secondary()
                  .by("age").create();
        });
        schema.indexLabel("person1ByAge2").onV("person1").shard()
              .by("age").create();
        schema.indexLabel("person1ByAge3").onV("person1").range()
              .by("age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person1ByAge");
        });
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person1ByAge2");
        });

        schema.vertexLabel("person2")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person2ByAge").onV("person2").shard()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person2ByAge1").onV("person2").secondary()
                  .by("age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person2ByAge2").onV("person2").shard()
                  .by("age").create();
        });
        schema.indexLabel("person2ByAge3").onV("person2").range()
              .by("age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person2ByAge");
        });

        schema.vertexLabel("person3")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person3ByCity").onV("person3").search()
              .by("city").create();
        schema.indexLabel("person3ByCity1").onV("person3").secondary()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person3ByCity2").onV("person3").search()
                  .by("city").create();
        });

        schema.vertexLabel("person4")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person4ByCity").onV("person4").secondary()
              .by("city").create();
        schema.indexLabel("person4ByCity1").onV("person4").search()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person4ByCity2").onV("person4").secondary()
                  .by("city").create();
        });

        schema.vertexLabel("person5")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person5ByCityAndName").onV("person5").secondary()
              .by("city", "name").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person5ByCity1").onV("person5").secondary()
                  .by("city").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person5ByCity2").onV("person5").shard()
                  .by("city").create();
        });
        schema.indexLabel("person5ByCity").onV("person5").search()
              .by("city").create();

        schema.vertexLabel("person6")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person6ByCityAndName").onV("person6").shard()
              .by("city", "name").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person6ByCity1").onV("person6").secondary()
                  .by("city").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person6ByCity2").onV("person6").shard()
                  .by("city").create();
        });
        schema.indexLabel("person6ByCity").onV("person6").search()
              .by("city").create();
    }

    @Test
    public void testAddIndexLabelWithSubIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.getIndexLabel("personByCity");
        schema.indexLabel("personByCityAndName").onV("person").secondary()
              .by("city", "name").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });

        schema.vertexLabel("person1")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person1ByCity").onV("person1").secondary()
              .by("city").create();
        schema.getIndexLabel("person1ByCity");
        schema.indexLabel("person1ByCityAndAge").onV("person1").secondary()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person1ByCity");
        });

        schema.vertexLabel("person2")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person2ByCity").onV("person2").shard()
              .by("city").create();
        schema.getIndexLabel("person2ByCity");
        schema.indexLabel("person2ByCityAndName").onV("person2").shard()
              .by("city", "name").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person2ByCity");
        });

        schema.vertexLabel("person3")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person3ByCity").onV("person3").shard()
              .by("city").create();
        schema.getIndexLabel("person3ByCity");
        schema.indexLabel("person3ByCityAndAge").onV("person3").shard()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person3ByCity");
        });

        schema.vertexLabel("person4")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person4ByCity").onV("person4").secondary()
              .by("city").create();
        schema.getIndexLabel("person4ByCity");
        schema.indexLabel("person4ByCityAndName").onV("person4").shard()
              .by("city", "name").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person4ByCity");
        });

        schema.vertexLabel("person5")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person5ByCity").onV("person5").shard()
              .by("city").create();
        schema.getIndexLabel("person5ByCity");
        schema.indexLabel("person5ByCityAndAge").onV("person5").secondary()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person5ByCity");
        });

        schema.vertexLabel("person6")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person6ByAge").onV("person6").secondary()
              .by("age").create();
        schema.getIndexLabel("person6ByAge");
        schema.indexLabel("person6ByAge1").onV("person6").range()
              .by("age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person6ByAge");
        });

        schema.vertexLabel("person7")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person7ByAge").onV("person7").shard()
              .by("age").create();
        schema.getIndexLabel("person7ByAge");
        schema.indexLabel("person7ByAge1").onV("person7").range()
              .by("age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person7ByAge");
        });
    }

    @Test
    public void testAddShardIndexLabelWithPrefixSecondaryIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();

        schema.getIndexLabel("personByCity");

        schema.indexLabel("personByCityAndAge").onV("person").shard()
              .by("city", "age").create();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });
    }

    @Test
    public void testRemoveIndexLabelOfVertex() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        VertexLabel person = schema.getVertexLabel("person");

        Assert.assertEquals(2, person.indexLabels().size());
        assertContainsIl(person.indexLabels(), "personByCity", "personByAge");

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        graph().tx().commit();

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

        person = schema.getVertexLabel("person");
        Assert.assertEquals(1, person.indexLabels().size());
        assertNotContainsIl(person.indexLabels(), "personByCity");
        assertContainsIl(person.indexLabels(), "personByAge");

        Assert.assertThrows(NoIndexException.class, () -> {
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
        Assert.assertEquals(0, person.indexLabels().size());

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("age", P.inside(2, 4)).next();
        });
    }

    @Test
    public void testRemoveIndexLabelOfEdge() {
        super.initPropertyKeys();
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

        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");

        james.addEdge("authored", java1,"contribution", "test");
        graph().tx().commit();

        Edge edge = graph().traversal().E().hasLabel("authored")
                    .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.indexLabel("authoredByContri").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("authoredByContri");
        });

        /*
         * Should not expect that schemalabel previously constructed can be
         * dynamically modified with index label operation
         */
        authored = schema.getEdgeLabel("authored");
        Assert.assertEquals(0, authored.indexLabels().size());

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().E().hasLabel("authored")
                   .has("contribution", "test").next();
        });
    }

    @Test
    public void testRemoveNotExistIndexLabel() {
        SchemaManager schema = graph().schema();
        schema.indexLabel("not-exist-il").remove();
    }

    @Test
    public void testRebuildIndexLabelOfVertex() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        VertexLabel person = schema.getVertexLabel("person");
        Assert.assertEquals(2, person.indexLabels().size());
        assertContainsIl(person.indexLabels(), "personByCity", "personByAge");

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        graph().tx().commit();

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
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();

        VertexLabel person = schema.getVertexLabel("person");
        Assert.assertEquals(2, person.indexLabels().size());
        assertContainsIl(person.indexLabels(), "personByCity", "personByAge");

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        graph().tx().commit();

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
        super.initPropertyKeys();
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

        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");

        james.addEdge("authored", java1,"contribution", "test");
        graph().tx().commit();

        Edge edge = graph().traversal().E().hasLabel("authored")
                    .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.indexLabel("authoredByContri").rebuild();
        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");

        edge = graph().traversal().E().hasLabel("authored")
               .has("contribution", "test").next();
        Assert.assertNotNull(edge);
    }

    @Test
    public void testRebuildIndexLabelOfEdge() {
        super.initPropertyKeys();
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

        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");

        james.addEdge("authored", java1,"contribution", "test");
        graph().tx().commit();

        Edge edge = graph().traversal().E().hasLabel("authored")
                    .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.edgeLabel("authored").rebuildIndex();
        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");
        edge = graph().traversal().E().hasLabel("authored")
               .has("contribution", "test").next();
        Assert.assertNotNull(edge);
    }
}
