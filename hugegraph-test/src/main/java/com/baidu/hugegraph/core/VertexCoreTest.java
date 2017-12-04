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

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.FakeObjects.FakeVertex;
import com.baidu.hugegraph.testutil.Utils;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Shard;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class VertexCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  propertyKey  ================");

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("dynamic").asBoolean().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueList().create();
        schema.propertyKey("contribution").asText().valueSet().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("cpu").asText().create();
        schema.propertyKey("ram").asText().create();
        schema.propertyKey("band").asText().create();
        schema.propertyKey("price").asInt().create();

        LOG.debug("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("age")
              .create();
        schema.vertexLabel("computer")
              .properties("name", "band", "cpu", "ram", "price")
              .primaryKeys("name", "band")
              .nullableKeys("ram", "cpu", "price")
              .ifNotExist()
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id")
              .nullableKeys("age", "lived")
              .create();
        schema.vertexLabel("language")
              .properties("name", "dynamic")
              .primaryKeys("name")
              .nullableKeys("dynamic")
              .create();
        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("review")
              .properties("id", "comment", "contribution")
              .primaryKeys("id")
              .nullableKeys("comment", "contribution")
              .create();

        LOG.debug("===============  vertexLabel index  ================");

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").search()
              .by("age").create();

        schema.indexLabel("pcByBand").onV("computer")
              .secondary().by("band")
              .ifNotExist()
              .create();
        schema.indexLabel("pcByCpuAndRamAndBand").onV("computer")
              .secondary().by("cpu", "ram", "band")
              .ifNotExist()
              .create();
    }

    @Test
    public void testAddVertex() {
        HugeGraph graph = graph();

        // Directly save into the backend
        graph.addVertex(T.label, "book", "name", "java-3");

        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3);
        graph.addVertex(T.label, "person", "name", "James",
                        "city", "Beijing", "age", 19);
        graph.addVertex(T.label, "person", "name", "Tom Cat",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Lisa",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Hebe",
                        "city", "Taipei", "age", 21);
        graph.tx().commit();

        long count = graph.traversal().V().count().next();
        Assert.assertEquals(6, count);
    }

    @Test
    public void testAddVertexWithInvalidPropertyType() {
        HugeGraph graph = graph();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book", "name", 18);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name", "Baby",
                            "city", "Hongkong", "age", "should-be-int");
        });
    }

    @Test
    public void testAddVertexWithPropertyList() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "review", "id", 1,
                                        "comment", "looks good!",
                                        "comment", "LGTM!");
        graph.tx().commit();

        vertex = vertex("review", "id", 1);
        Assert.assertEquals(ImmutableList.of("looks good!", "LGTM!"),
                            vertex.value("comment"));
        List<Object> props = vertex.value("comment");
        Assert.assertEquals(2, props.size());
        Assert.assertEquals("looks good!", props.get(0));
        Assert.assertEquals("LGTM!", props.get(1));

        vertex = graph.addVertex(T.label, "review", "id", 2,
                                 "comment",
                                 ImmutableList.of("looks good 2!", "LGTM!"));
        graph.tx().commit();

        vertex = vertex("review", "id", 2);
        Assert.assertEquals(ImmutableList.of("looks good 2!", "LGTM!"),
                            vertex.value("comment"));
        props = vertex.value("comment");
        Assert.assertEquals(2, props.size());
        Assert.assertEquals("looks good 2!", props.get(0));
        Assert.assertEquals("LGTM!", props.get(1));

        vertex = graph.addVertex(T.label, "review", "id", 3,
                                 "comment",
                                 new String[]{"looks good 3!", "LGTM!"});
        graph.tx().commit();

        vertex = vertex("review", "id", 3);
        Assert.assertEquals(ImmutableList.of("looks good 3!", "LGTM!"),
                            vertex.value("comment"));
        props = vertex.value("comment");
        Assert.assertEquals(2, props.size());
        Assert.assertEquals("looks good 3!", props.get(0));
        Assert.assertEquals("LGTM!", props.get(1));
    }

    @Test
    public void testAddVertexWithInvalidPropertyList() {
        HugeGraph graph = graph();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 1,
                            "comment", "looks good!",
                            "comment", 18);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 2,
                            "comment", ImmutableList.of("looks good 2!", 18));
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Object[] comments = new Object[]{"looks good 3!", "3"};
            comments[1] = 3;
            graph.addVertex(T.label, "review", "id", 3,
                            "comment", comments);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 3,
                            "comment", new int[]{1, 2});
        });
    }

    @Test
    public void testAddVertexWithPropertySet() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "review", "id", 1,
                                        "contribution", "+1",
                                        "contribution", "+2",
                                        "contribution", "+2");
        graph.tx().commit();

        vertex = vertex("review", "id", 1);
        Assert.assertEquals(ImmutableSet.of("+1", "+2"),
                            vertex.value("contribution"));

        vertex = graph.addVertex(T.label, "review", "id", 2,
                                 "contribution",
                                 ImmutableSet.of("+1", "+1", "+2"));
        graph.tx().commit();

        vertex = vertex("review", "id", 2);
        Assert.assertEquals(ImmutableSet.of("+1", "+2"),
                            vertex.value("contribution"));
    }

    @Test
    public void testAddVertexWithInvalidPropertySet() {
        HugeGraph graph = graph();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 1,
                            "contribution", "+1",
                            "contribution", 2);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 2,
                            "contribution", ImmutableSet.of("+1", 2));
        });
    }

    @Test
    public void testAddVertexWithInvalidVertexLabelType() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, true);
        });
    }

    @Test
    public void testAddVertexWithNotExistsVertexLabel() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "not-exists-label");
        });
    }

    @Test
    public void testAddVertexWithNotExistsPropKey() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book", "not-exists-porp", "test");
        });
    }

    @Test
    public void testAddVertexWithNullableKeyAbsent() {
        Vertex vertex = graph().addVertex(T.label, "person", "name", "Baby",
                                          "city", "Hongkong");
        Assert.assertEquals("Baby", vertex.value("name"));
        Assert.assertEquals("Hongkong", vertex.value("city"));
    }

    @Test
    public void testAddVertexLabelNewVertexWithNonNullKeysAbsent() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'city'
            graph().addVertex(T.label, "person", "name", "Baby", "age", 18);
        });
    }

    @Test
    public void testAddVertexWithAppendedNullableKeysAbsent() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").nullableKeys("city").append();
        // Absent 'age' and 'city'
        Vertex vertex = graph().addVertex(T.label, "person", "name", "Baby");
        Assert.assertEquals("Baby", vertex.value("name"));
    }

    @Test
    public void testAddVertexWithNotExistsVertexPropKey() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book", "age", 12);
        });
    }

    @Test
    public void testAddVertexWithoutPrimaryValues() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book");
        });
    }

    @Test
    public void testAddVertexWithoutVertexLabel() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex("name", "test");
        });
    }

    @Test
    public void testAddVertexWithAutomaticIdStrategyButPassedId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useAutomaticId()
              .properties("name", "age", "city")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", T.id, "123456",
                            "name", "marko", "age", 18, "city", "Beijing");
        });
    }

    @Test
    public void testAddVertexWithAutomaticIdStrategyAndNotPassedId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useAutomaticId()
              .properties("name", "age", "city")
              .create();

        graph.addVertex(T.label, "programmer", "name", "marko",
                        "age", 18, "city", "Beijing");
        graph.tx().commit();

        List<Vertex> vertices = graph.traversal().V().toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");
    }

    @Test
    public void testAddVertexWithPrimaryKeyIdStrategyButPassedId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .usePrimaryKeyId()
              .properties("name", "age", "city")
              .primaryKeys("name", "age")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", T.id, "123456",
                            "name", "marko", "age", 18, "city", "Beijing");
        });
    }

    @Test
    public void testAddVertexWithPrimaryKeyIdStrategy() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .usePrimaryKeyId()
              .properties("name", "age", "city")
              .primaryKeys("name", "age")
              .create();
        graph.addVertex(T.label, "programmer", "name", "marko",
                        "age", 18, "city", "Beijing");
        graph.tx().commit();

        List<Vertex> vertices = graph.traversal().V("programmer:marko!18")
                                .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("programmer:marko!18",
                            vertices.get(0).id().toString());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");
    }

    @Test
    public void testAddVertexWithCustomizeIdStrategyAndPassedId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useCustomizeId()
              .properties("name", "age", "city")
              .create();
        graph.addVertex(T.label, "programmer", T.id, "123456", "name", "marko",
                        "age", 18, "city", "Beijing");
        graph.tx().commit();

        List<Vertex> vertices = graph.traversal().V("123456").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("123456", vertices.get(0).id().toString());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");
    }

    @Test
    public void testAddVertexWithCustomizeIdStrategyButNotPassedId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useCustomizeId()
              .properties("name", "age", "city")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", "name", "marko",
                            "age", 18, "city", "Beijing");
        });
    }

    @Test
    public void testAddVertexWithTx() {
        HugeGraph graph = graph();
        GraphTransaction tx = graph.openTransaction();

        tx.addVertex(T.label, "book", "name", "java-4");
        tx.addVertex(T.label, "book", "name", "java-5");

        try {
            tx.commit();
        } finally {
            tx.close();
        }

        long count = graph.traversal().V().count().next();
        Assert.assertEquals(2, count);
    }

    @Test
    public void testQueryAll() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query all
        List<Vertex> vertexes = graph.traversal().V().toList();

        Assert.assertEquals(10, vertexes.size());

        assertContains(vertexes,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        assertContains(vertexes, T.label, "language", "name", "java");

        assertContains(vertexes, T.label, "book", "name", "java-1");
    }

    @Test
    public void testQueryAllWithLimit() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query all with limit
        List<Vertex> vertexes = graph.traversal().V().limit(3).toList();

        Assert.assertEquals(3, vertexes.size());
    }

    @Test
    public void testQueryAllWithLimit0() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query all with limit 0
        List<Vertex> vertexes = graph.traversal().V().limit(0).toList();

        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryAllWithNoLimit() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query all with limit -1 (also no-limit)
        List<Vertex> vertexes = graph.traversal().V().limit(-1).toList();

        Assert.assertEquals(10, vertexes.size());
    }

    @Test
    public void testSplicingId() {
        HugeGraph graph = graph();
        init10Vertices();
        List<Vertex> vertexes = graph.traversal().V().toList();

        Assert.assertTrue(Utils.containsId(vertexes,
                          SplicingIdGenerator.splicing("book", "java-1")));
        Assert.assertTrue(Utils.containsId(vertexes,
                          SplicingIdGenerator.splicing("book", "java-3")));
        Assert.assertTrue(Utils.containsId(vertexes,
                          SplicingIdGenerator.splicing("book", "java-5")));
    }

    @Test
    public void testQueryById() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query vertex by id
        Id id = SplicingIdGenerator.splicing("author", "1");
        List<Vertex> vertexes = graph.traversal().V(id).toList();
        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");
    }

    @Test
    public void testQueryByIdNotFound() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query vertex by id which not exists
        Id id = SplicingIdGenerator.splicing("author", "not-exists-id");
        Assert.assertTrue(graph.traversal().V(id).toList().isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            graph.traversal().V(id).next();
        });
    }

    @Test
    public void testQueryByLabel() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query by vertex label
        List<Vertex> vertexes = graph.traversal().V().hasLabel("book")
                                .toList();

        Assert.assertEquals(5, vertexes.size());

        Assert.assertTrue(Utils.containsId(vertexes,
                          SplicingIdGenerator.splicing("book", "java-1")));
        Assert.assertTrue(Utils.containsId(vertexes,
                          SplicingIdGenerator.splicing("book", "java-2")));
        Assert.assertTrue(Utils.containsId(vertexes,
                          SplicingIdGenerator.splicing("book", "java-3")));
        Assert.assertTrue(Utils.containsId(vertexes,
                          SplicingIdGenerator.splicing("book", "java-4")));
        Assert.assertTrue(Utils.containsId(vertexes,
                          SplicingIdGenerator.splicing("book", "java-5")));
    }

    @Test
    public void testQueryByLabelNotExists() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query by not exists vertex label
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("xx").toList();
        });
    }

    @Test
    public void testQueryByLabelAndKeyName() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query by vertex label and key-name
        List<Vertex> vertexes = graph.traversal().V().hasLabel("language")
                                .has("dynamic").toList();

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "language", "name", "python", "dynamic", true);
    }

    @Test
    public void testQueryByPrimaryValues() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query vertex by primary-values
        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("author").has("id", 1).toList();
        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");
    }

    @Test
    public void testQueryFilterByPropName() {
        HugeGraph graph = graph();
        BackendFeatures features = graph.graphTransaction().store().features();
        Assume.assumeTrue("Not support CONTAINS_KEY query",
                          features.supportsQueryByContainsKey());
        init10Vertices();

        // Query vertex by condition (does contain the property name?)
        ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
        q.eq(HugeKeys.LABEL, "language");
        q.key(HugeKeys.PROPERTIES, "dynamic");
        List<Vertex> vertexes = ImmutableList.copyOf(graph.vertices(q));

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "language", "name", "python", "dynamic", true);
    }

    @Test
    public void testQueryByHasKey() {
        HugeGraph graph = graph();
        BackendFeatures features = graph.graphTransaction().store().features();
        Assume.assumeTrue("Not support CONTAINS_KEY query",
                          features.supportsQueryByContainsKey());
        init10Vertices();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("language").hasKey("dynamic")
                                .toList();
        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "language", "name", "python",
                       "dynamic", true);

        vertexes = graph.traversal().V().hasKey("age").toList();
        Assert.assertEquals(2, vertexes.size());
        assertContains(vertexes,
                       T.label, "author", "id", 1,
                       "name", "James Gosling", "age", 62,
                       "lived", "Canadian");
        assertContains(vertexes,
                       T.label, "author", "id", 2,
                       "name", "Guido van Rossum", "age", 61,
                       "lived", "California");
    }

    @Test
    public void testQueryByHasKeys() {
        HugeGraph graph = graph();
        init10Vertices();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("language")
                 .hasKey("dynamic", "name").toList();
        });
    }

    @Test
    public void testQueryByHasValue() {
        HugeGraph graph = graph();
        BackendFeatures features = graph.graphTransaction().store().features();
        Assume.assumeTrue("Not support CONTAINS query",
                          features.supportsQueryByContains());
        init10Vertices();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("language").hasValue(true)
                                .toList();
        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "language", "name", "python",
                       "dynamic", true);

        vertexes = graph.traversal().V().hasValue(62).toList();
        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "author", "id", 1,
                       "name", "James Gosling", "age", 62,
                       "lived", "Canadian");
    }

    @Test
    public void testQueryByHasValues() {
        HugeGraph graph = graph();
        init10Vertices();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("language")
                 .hasValue("python", true).toList();
        });
    }

    @Test
    public void testQueryByStringPropWithOneResult() {
        // city is "Taipei"
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("city", "Taipei")
                                .toList();

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "person", "name", "Hebe",
                       "city", "Taipei", "age", 21);
    }

    @Test
    public void testQueryByStringPropWithMultiResults() {
        // NOTE: InMemoryDBStore would fail due to it not support index ele-ids

        // city is "Beijing"
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("city", "Beijing")
                                .toList();

        Assert.assertEquals(3, vertexes.size());

        assertContains(vertexes,
                       T.label, "person", "name", "James",
                       "city", "Beijing", "age", 19);
        assertContains(vertexes,
                       T.label, "person", "name", "Tom Cat",
                       "city", "Beijing", "age", 20);
        assertContains(vertexes,
                       T.label, "person", "name", "Lisa",
                       "city", "Beijing", "age", 20);
    }

    @Test
    public void testQueryByIntPropWithOneResult() {
        // age = 19
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", 19).toList();

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "person", "name", "James",
                       "city", "Beijing", "age", 19);
    }

    @Test
    public void testQueryByIntPropWithMultiResults() {
        // age = 20
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", 20).toList();

        Assert.assertEquals(2, vertexes.size());

        assertContains(vertexes,
                       T.label, "person", "name", "Tom Cat",
                       "city", "Beijing", "age", 20);
        assertContains(vertexes,
                       T.label, "person", "name", "Lisa",
                       "city", "Beijing", "age", 20);
    }

    @Test
    public void testQueryByIntPropWithNonResult() {
        // age = 18
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", 18).toList();

        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryByIntPropWithDifferentDataType() {
        Assume.assumeTrue("Not support search condition query",
                          storeFeatures().supportsQueryWithSearchCondition());
        HugeGraph graph = graph();
        init5Persons();
        List<Vertex> vertexes;

        vertexes = graph.traversal().V().has("age", 21).toList();
        Assert.assertEquals(1, vertexes.size());

        vertexes = graph.traversal().V().has("age", 21.0).toList();
        Assert.assertEquals(1, vertexes.size());

        vertexes = graph.traversal().V().has("age", "21").toList();
        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingLtWithOneResult() {
        // age < 19
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                               .hasLabel("person").has("age", P.lt(19))
                               .toList();

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "person", "name", "Baby",
                       "city", "Hongkong", "age", 3);
    }

    @Test
    public void testQueryByIntPropUsingLtWithMultiResults() {
        // age < 21
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", P.lt(21))
                                .toList();

        Assert.assertEquals(4, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingLteWithMultiResults() {
        // age <= 20
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", P.lte(20))
                                .toList();

        Assert.assertEquals(4, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingGtWithOneResult() {
        // age > 20
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", P.gt(20))
                                .toList();

        Assert.assertEquals(1, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingGtWithMultiResults() {
        // age > 1
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", P.gt(1))
                                .toList();

        Assert.assertEquals(5, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingGtWithNonResult() {
        // age > 30
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", P.gt(30))
                                .toList();

        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingGteWithMultiResults() {
        // age >= 20
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", P.gte(20))
                                .toList();

        Assert.assertEquals(3, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingInsideWithOneResult() {
        Assume.assumeTrue("Not support search condition query",
                          storeFeatures().supportsQueryWithSearchCondition());
        HugeGraph graph = graph();
        init5Persons();

        // 3 < age && age < 20 (that's age == 19)
        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person").has("age", P.inside(3, 20))
                                .toList();

        Assert.assertEquals(1, vertexes.size());
        Assert.assertEquals(19, vertexes.get(0).property("age").value());
    }

    @Test
    public void testQueryByIntPropUsingInsideWithMultiResults() {
        Assume.assumeTrue("Not support search condition query",
                          storeFeatures().supportsQueryWithSearchCondition());
        HugeGraph graph = graph();
        init5Persons();

        // 19 < age && age < 21 (that's age == 20)
        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person")
                                .has("age", P.inside(19, 21))
                                .toList();

        Assert.assertEquals(2, vertexes.size());

        // 3 < age && age < 21 (that's age == 19 or age == 20)
        vertexes = graph.traversal().V()
                   .hasLabel("person").has("age", P.inside(3, 21))
                   .toList();

        Assert.assertEquals(3, vertexes.size());

        // 0 < age && age < 22 (that's all)
        vertexes = graph.traversal().V()
                   .hasLabel("person")
                   .has("age", P.inside(0, 22))
                   .toList();

        Assert.assertEquals(5, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingInsideWithNonResult() {
        Assume.assumeTrue("Not support search condition query",
                          storeFeatures().supportsQueryWithSearchCondition());
        HugeGraph graph = graph();
        init5Persons();

        // 3 < age && age < 19
        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person")
                                .has("age", P.inside(3, 19))
                                .toList();

        Assert.assertEquals(0, vertexes.size());

        // 0 < age && age < 3
        vertexes = graph.traversal().V()
                   .hasLabel("person").has("age", P.inside(0, 3))
                   .toList();

        Assert.assertEquals(0, vertexes.size());

        // 20 < age && age < 21
        vertexes = graph.traversal().V()
                   .hasLabel("person").has("age", P.inside(20, 21))
                   .toList();

        Assert.assertEquals(0, vertexes.size());

        // 21 < age && age < 25
        vertexes = graph.traversal().V()
                   .hasLabel("person").has("age", P.inside(21, 25))
                   .toList();

        Assert.assertEquals(0, vertexes.size());

        // 21 < age && age < 20
        vertexes = graph.traversal().V()
                   .hasLabel("person")
                   .has("age", P.inside(21, 20))
                   .toList();

        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingBetweenWithOneResult() {
        Assume.assumeTrue("Not support search condition query",
                          storeFeatures().supportsQueryWithSearchCondition());
        HugeGraph graph = graph();
        init5Persons();

        // 3 <= age && age < 19 (that's age == 3)
        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person")
                                .has("age", P.between(3, 19))
                                .toList();

        Assert.assertEquals(1, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingBetweenWithMultiResults() {
        Assume.assumeTrue("Not support search condition query",
                          storeFeatures().supportsQueryWithSearchCondition());
        HugeGraph graph = graph();
        init5Persons();

        // 19 <= age && age < 21
        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person")
                                .has("age", P.between(19, 21))
                                .toList();

        Assert.assertEquals(3, vertexes.size());

        // 3 <= age && age < 21
        vertexes = graph.traversal().V()
                   .hasLabel("person").has("age", P.between(3, 21))
                   .toList();

        Assert.assertEquals(4, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingBetweenWithNonResult() {
        Assume.assumeTrue("Not support search condition query",
                          storeFeatures().supportsQueryWithSearchCondition());
        HugeGraph graph = graph();
        init5Persons();

        // 4 <= age && age < 19
        List<Vertex> vertexes = graph.traversal().V()
                                .hasLabel("person")
                                .has("age", P.between(4, 19))
                                .toList();

        Assert.assertEquals(0, vertexes.size());

        // 3 <= age && age < 3
        vertexes = graph.traversal().V()
                   .hasLabel("person").has("age", P.between(3, 3))
                   .toList();

        Assert.assertEquals(0, vertexes.size());

        // 21 <= age && age < 20
        vertexes = graph.traversal().V()
                   .hasLabel("person").has("age", P.between(21, 20))
                   .toList();

        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryWithMultiLayerConditions() {
        HugeGraph graph = graph();
        init5Persons();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("person").has(
                    "age",
                    P.not(P.lte(10).and(P.not(P.between(11, 20))))
                     .and(P.lt(29).or(P.eq(35))))
                    .values("name").next();
        });
    }

    @Test
    public void testQueryByIntPropOfOverrideVertex() {
        HugeGraph graph = graph();
        graph.addVertex(T.label, "person", "name", "Zhangyi",
                        "city", "Beijing", "age", 28);
        graph.addVertex(T.label, "person", "name", "Zhangyi",
                        "city", "Hongkong", "age", 29);
        graph.tx().commit();

        List<Vertex> vertices = graph.traversal().V().has("age", 28).toList();
        Assert.assertEquals(0, vertices.size());
        vertices = graph.traversal().V().has("age", 29).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByStringPropOfOverrideVertex() {
        HugeGraph graph = graph();
        graph.addVertex(T.label, "person", "name", "Zhangyi",
                        "city", "Beijing", "age", 28);
        graph.addVertex(T.label, "person", "name", "Zhangyi",
                        "city", "Hongkong", "age", 29);
        graph.tx().commit();

        List<Vertex> vertices = graph.traversal().V().has("city", "Beijing")
                                     .toList();
        Assert.assertEquals(0, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryWithTxNotCommittedByNoCondition() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");
        graph.addVertex(T.label, "person", "name", "james",
                        "age", 19, "city", "Hongkong");

        List<Vertex> vertices;
        vertices = graph.traversal().V().toList();
        Assert.assertEquals(2, vertices.size());

        graph.tx().rollback();
        vertices = graph.traversal().V().toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryWithTxNotCommittedById() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");

        List<Vertex> vertices;
        vertices = graph.traversal().V("person:marko").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("person:marko",
                            vertices.get(0).id().toString());

        graph.tx().rollback();
        vertices = graph.traversal().V("person:marko").toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryWithTxNotCommittedByIntProp() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");

        List<Vertex> vertices;
        vertices = graph.traversal().V().has("age", 18).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(18, (int) vertices.get(0).<Integer>value("age"));

        graph.tx().rollback();
        vertices = graph.traversal().V("person:marko").toList();
        Assert.assertEquals(0, vertices.size());

        // TODO: add test for: append, eliminate, remove
    }

    @Test
    public void testQueryWithTxNotCommittedByIdInOtherThread()
                throws InterruptedException {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");

        AtomicInteger size = new AtomicInteger(-1);
        Thread t = new Thread(() -> {
            List<Vertex> vertices = graph.traversal()
                                         .V("person:marko")
                                         .toList();
            size.set(vertices.size());
        });
        t.start();
        t.join();
        Assert.assertEquals(0, size.get());
    }

    @Test
    public void testRemoveVertex() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertexes = graph.traversal().V().toList();
        Assert.assertEquals(10, vertexes.size());
        assertContains(vertexes,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        Vertex vertex = vertex("author", "id", 1);
        vertex.remove();
        graph.tx().commit();

        vertexes = graph.traversal().V().toList();
        Assert.assertEquals(9, vertexes.size());
        assertNotContains(vertexes,
                          T.label, "author", "id", 1, "name", "James Gosling",
                          "age", 62, "lived", "Canadian");
    }

    @Test
    public void testRemoveVertexOfNotExists() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertexes = graph.traversal().V().toList();
        Assert.assertEquals(10, vertexes.size());
        assertContains(vertexes,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        Vertex vertex = vertex("author", "id", 1);
        vertex.remove();
        graph.tx().commit();

        vertexes = graph.traversal().V().toList();
        Assert.assertEquals(9, vertexes.size());
        assertNotContains(vertexes,
                          T.label, "author", "id", 1, "name", "James Gosling",
                          "age", 62, "lived", "Canadian");

        // Remove again
        vertex.remove();
        graph.tx().commit();
    }

    public void testRemoveVertexAfterAddVertexWithTx() {
        HugeGraph graph = graph();
        GraphTransaction tx = graph.openTransaction();

        Vertex java1 = tx.addVertex(T.label, "book", "name", "java-1");
        tx.addVertex(T.label, "book", "name", "java-2");
        tx.commit();
        java1.remove();
        tx.commit();
        tx.close();

        List<Vertex> vertexes = graph.traversal().V().toList();
        Assert.assertEquals(1, vertexes.size());
        assertNotContains(vertexes, T.label, "book", "name", "java-2");
    }

    @Test
    public void testRemoveVertexOneByOne() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertexes = graph.traversal().V().toList();
        Assert.assertEquals(10, vertexes.size());

        for (int i = 0; i < vertexes.size(); i++) {
            vertexes.get(i).remove();
            graph.tx().commit();
            Assert.assertEquals(9 - i, graph.traversal().V().toList().size());
        }
    }

    @Test
    public void testAddVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom");
        graph.tx().commit();

        // Add properties
        vertex.property("name", "Tom2");
        vertex.property("age", 10);
        vertex.property("lived", "USA");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tom2", vertex.property("name").value());
        Assert.assertEquals(10, vertex.property("age").value());
        Assert.assertEquals("USA", vertex.property("lived").value());
    }

    @Test
    public void testAddVertexPropertyExisted() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tom", vertex.property("name").value());

        vertex.property("name", "Tom2");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tom2", vertex.property("name").value());
    }

    @Test
    public void testAddVertexPropertyExistedWithIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Tom",
                                        "city", "Hongkong", "age", 3);
        graph.tx().commit();

        vertex = vertex("person", "name", "Tom");
        Assert.assertEquals(3, vertex.property("age").value());

        vertex.property("age", 4);
        graph.tx().commit();

        Assert.assertEquals(4, vertex.property("age").value());
        vertex = vertex("person", "name", "Tom");
        Assert.assertEquals(4, vertex.property("age").value());
    }

    @Test
    public void testAddVertexPropertyNotInVertexLabel() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("time", "2017-1-1");
        });
    }

    @Test
    public void testAddVertexPropertyWithNotExistPropKey() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Jim");
        graph.tx().commit();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("prop-not-exist", "2017-1-1");
        });
    }

    @Test
    public void testAddVertexPropertyOfPrimaryKey() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Jim");
        graph.tx().commit();

        Assert.assertEquals(1, vertex.property("id").value());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Update primary key property
            vertex.property("id", 2);
        });
    }

    @Test
    public void testRemoveVertexProperty() {
        HugeGraph graph = graph();
        Vertex v = graph.addVertex(T.label, "author", "id", 1,
                                   "name", "Tom", "age", 10, "lived", "USA");
        graph.tx().commit();

        // Remove "name" property
        Assert.assertTrue(v.property("name").isPresent());
        Assert.assertTrue(v.property("lived").isPresent());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            v.property("name").remove();
            graph.tx().commit();
        });
        Assert.assertTrue(v.property("name").isPresent());
        Assert.assertTrue(v.property("lived").isPresent());

        // Remove "lived" property
        Vertex vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("name").isPresent());
        Assert.assertTrue(vertex.property("lived").isPresent());
        vertex.property("lived").remove();
        graph.tx().commit();
        Assert.assertTrue(vertex.property("name").isPresent());
        Assert.assertFalse(vertex.property("lived").isPresent());

        vertex = vertex("author", "id", 1);
        Assert.assertEquals(10, vertex.property("age").value());
        Assert.assertTrue(vertex.property("name").isPresent());
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    @Test
    public void testRemoveVertexPropertyOfPrimaryKey() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "age", 10,
                                        "lived", "USA");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("id").remove();
        });
    }

    @Test
    public void testRemoveVertexPropertyNullableWithIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        vertex.property("age").remove();
    }

    @Test
    public void testRemoveVertexPropertyNonNullWithIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("name").remove();
        });
    }

    @Test
    public void testRemoveVertexPropertyNullableWithoutIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Baby", "lived", "Hongkong");
        vertex.property("age").remove();
    }

    @Test
    public void testRemoveVertexPropertyNonNullWithoutIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Baby", "lived", "Hongkong");
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("name").remove();
        });
    }

    @Test
    public void testAddVertexPropertyWithIllegalValueForIndex() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name", "Baby",
                            "city", "\u0000", "age", 3);
            graph.tx().commit();
        }, (e) -> {
            Assert.assertTrue(e.getMessage().contains(
                              "Illegal value of text property: '\u0000'"));
        });
    }

    @Test
    public void testQueryVertexByPropertyWithEmptyString() {
        HugeGraph graph = graph();
        graph.addVertex(T.label, "person", "name", "Baby", "city", "");
        graph.tx().commit();

        Vertex vertex = graph.traversal().V().has("city", "").next();
        Assert.assertEquals("Baby", vertex.value("name"));
        Assert.assertEquals("", vertex.value("city"));
    }

    @Test
    public void testQueryVertexBeforeAfterUpdateMultiPropertyWithIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        graph.tx().commit();

        List<Vertex> vl = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("age", 5).toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("city", "Shanghai").toList();
        Assert.assertEquals(0, vl.size());

        vertex.property("age", 5);
        vertex.property("city", "Shanghai");
        graph.tx().commit();

        vl = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("age", 5).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("city", "Shanghai").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
    }

    @Test
    public void testQueryVertexBeforeAfterUpdatePropertyWithSecondaryIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        graph.tx().commit();

        List<Vertex> vl = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("city", "Shanghai").toList();
        Assert.assertEquals(0, vl.size());

        vertex.property("city", "Shanghai");
        graph.tx().commit();

        vl = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("city", "Shanghai").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
    }

    @Test
    public void testQueryVertexBeforeAfterUpdatePropertyWithSearchIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        graph.tx().commit();

        List<Vertex> vl = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("age", 5).toList();
        Assert.assertEquals(0, vl.size());

        vertex.property("age", 5);
        graph.tx().commit();

        vl = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("age", 5).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
    }

    @Test
    public void testQueryVertexWithNullablePropertyInJointIndex() {
        HugeGraph graph = graph();
        graph.addVertex(T.label, "computer", "name", "1st", "band", "10Gbps",
                        "cpu", "2GHz", "ram", "8GB", "price", 1000);
        graph.tx().commit();


        List<Vertex> vl = graph.traversal().V().has("cpu", "2GHz").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("1st", vl.get(0).value("name"));

        vl = graph.traversal().V().has("cpu", "2GHz")
                  .has("ram", "8GB").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("1st", vl.get(0).value("name"));
    }

    @Test
    public void testUpdateVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        graph.tx().commit();

        vertex.property("lived").remove();
        vertex.property("lived", "Shanghai");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testUpdateVertexPropertyTwice() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        graph.tx().commit();

        vertex.property("lived").remove();
        vertex.property("lived").remove();

        vertex.property("lived", "Hangzhou");
        vertex.property("lived", "Shanghai");

        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testUpdateVertexPropertyOfNewVertex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");

        vertex.property("lived").remove();
        vertex.property("lived", "Shanghai");

        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testUpdateVertexPropertyOfPrimaryKey() {
        HugeGraph graph = graph();
        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Beijing");
        graph.tx().commit();

        Vertex vertex = vertex("author", "id", 1);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Update primary key property
            vertex.property("id", 2);
        });
    }

    @Test
    public void testUpdateVertexPropertyOfPrimaryKeyOfNewVertex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Update primary key property
            vertex.property("id", 2);
        });
    }

    @Test
    public void testUpdateVertexPropertyOfAddingVertex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        graph.tx().commit();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Beijing");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived", "Shanghai");
        });
    }

    @Test
    public void testUpdateVertexPropertyOfRemovingVertex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        graph.tx().commit();

        vertex.remove();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived", "Shanghai");
        });
    }

    @Test
    public void testUpdateVertexPropertyOfRemovingVertexWithDrop() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        graph.tx().commit();

        graph.traversal().V(vertex.id()).drop().iterate();

        // Update on dirty vertex
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived", "Shanghai");
        });
    }

    // Insert -> {Insert, Delete, Append, Eliminate}
    @Test
    public void testInsertAndInsertVertex() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Beijing");
        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Shanghai");
        graph.tx().commit();

        Vertex vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testInsertAndDeleteVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.remove();
        graph.tx().commit();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testInsertAndAppendVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Shanghai");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testInsertAndEliminateVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    // Delete -> {Insert, Delete, Append, Eliminate}
    @Test
    public void testDeleteAndInsertVertex() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Beijing");

        graph.traversal().V().hasLabel("author").has("id", 1).next().remove();
        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Shanghai");
        graph.tx().commit();

        Vertex vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("lived").isPresent());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testDeleteAndDeleteVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.remove();
        vertex.remove();
        graph.tx().commit();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testDeleteAndAppendVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.remove();
        vertex.property("lived", "Shanghai");
        graph.tx().commit();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testDeleteAndEliminateVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.remove();
        vertex.property("lived").remove();
        graph.tx().commit();

        Assert.assertNull(vertex("author", "id", 1));
    }

    // Append -> {Insert, Delete, Append, Eliminate}
    @Test
    public void testAppendAndInsertVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Wuhan");
        graph.addVertex(T.label, "author", "id", 1, "name", "Tom",
                        "lived", "Shanghai");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("lived").isPresent());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testAppendAndDeleteVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Wuhan");
        vertex.remove();
        graph.tx().commit();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testAppendAndAppendSameVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Wuhan");
        vertex.property("lived", "Shanghai");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testAppendAndAppendDifferentVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("name", "Tomcat");
        vertex.property("lived", "Shanghai");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tomcat", vertex.property("name").value());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testAppendAndEliminateSameVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Shanghai");
        vertex.property("lived").remove();
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    @Test
    public void testAppendAndEliminateDifferentVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("name", "Tomcat");
        vertex.property("lived").remove();
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
        Assert.assertEquals("Tomcat", vertex.property("name").value());
    }

    // Eliminate -> {Insert, Delete, Append, Eliminate}
    @Test
    public void testEliminateAndInsertVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        graph.addVertex(T.label, "author", "id", 1, "name", "Tom",
                        "lived", "Shanghai");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("lived").isPresent());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testEliminateAndDeleteVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        vertex.remove();
        graph.tx().commit();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testEliminateAndAppendSameVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        vertex.property("lived", "Shanghai");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("lived").isPresent());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testEliminateAndAppendDifferentVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");

        vertex.property("lived").remove();
        vertex.property("name", "Tomcat");
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
        Assert.assertEquals("Tomcat", vertex.property("name").value());
    }

    @Test
    public void testEliminateAndEliminateSameVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        vertex.property("lived").remove();
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    @Test
    public void testEliminateAndEliminateDifferentVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1, "age", 18,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("age").remove();
        vertex.property("lived").remove();
        graph.tx().commit();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("age").isPresent());
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testScanVertex() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan", storeFeatures().supportsScan());
        init10Vertices();

        List<Vertex> vertexes = new LinkedList<>();

        long splitSize = 1 * 1024 * 1024;
        Object splits = graph.graphTransaction()
                        .metadata(HugeType.VERTEX, "splits", splitSize);
        for (Shard split : (List<Shard>) splits) {
            ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
            q.scan(split.start(), split.end());
            vertexes.addAll(ImmutableList.copyOf(graph.vertices(q)));
        }

        Assert.assertEquals(10, vertexes.size());
    }

    @Test
    public void testScanVertexWithSplitSizeLt1MB() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan", storeFeatures().supportsScan());
        init10Vertices();

        long splitSize = 1 * 1024 * 1024 - 1;
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.graphTransaction()
                 .metadata(HugeType.VERTEX, "splits", splitSize);
        });
    }

    @Test
    public void testScanVertexWithSplitSizeTypeError() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan", storeFeatures().supportsScan());
        init10Vertices();

        String splitSize = "123456";
        Assert.assertThrows(ClassCastException.class, () -> {
            graph.graphTransaction()
                 .metadata(HugeType.VERTEX, "splits", splitSize);
        });
    }

    @Test
    public void testScanVertexWithoutSplitSize() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan", storeFeatures().supportsScan());
        init10Vertices();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.graphTransaction().metadata(HugeType.VERTEX, "splits");
        });
    }

    @Test
    public void testQuerySingleIndexedPropertyByEqual() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertexes = graph.traversal().V()
                                .has("band", "lenovo").toList();
        Assert.assertEquals(2, vertexes.size());

        vertexes = graph.traversal().V().has("band", "apple").toList();
        Assert.assertEquals(1, vertexes.size());
    }

    @Test
    public void testQuerySingleIndexedPropertyByNotEqual() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertexes = graph.traversal().V()
                                .has("band", "acer").toList();
        Assert.assertEquals(0, vertexes.size());

        vertexes = graph.traversal().V().has("band", "Hp").toList();
        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByEqualOnePrefix() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertexes = graph.traversal().V()
                                .has("cpu", "3.2GHz").toList();
        Assert.assertEquals(3, vertexes.size());

        vertexes = graph.traversal().V().has("cpu", "4.6GHz").toList();
        Assert.assertEquals(1, vertexes.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByNotEqualOnePrefix() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertexes = graph.traversal().V()
                                .has("cpu", "2.8GHz").toList();
        Assert.assertEquals(0, vertexes.size());

        vertexes = graph.traversal().V().has("cpu", "4.8GHz").toList();
        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByEqualTwoPrefix() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertexes = graph.traversal().V()
                                .has("cpu", "3.2GHz")
                                .has("ram", "16GB")
                                .toList();
        Assert.assertEquals(2, vertexes.size());

        vertexes = graph.traversal().V()
                   .has("ram", "32GB")
                   .has("cpu", "4.6GHz")
                   .toList();
        Assert.assertEquals(1, vertexes.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByNotEqualTwoPrefix() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertexes = graph.traversal().V()
                                .has("cpu", "3.3GHz")
                                .has("ram", "16GB")
                                .toList();
        Assert.assertEquals(0, vertexes.size());

        vertexes = graph.traversal().V()
                   .has("ram", "32GB")
                   .has("cpu", "4.8GHz")
                   .toList();
        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByEqualAll() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertexes = graph.traversal().V()
                                .has("cpu", "3.2GHz")
                                .has("band", "lenovo")
                                .has("ram", "16GB")
                                .toList();
        Assert.assertEquals(1, vertexes.size());

        vertexes = graph.traversal().V()
                   .has("ram", "32GB")
                   .has("cpu", "4.6GHz")
                   .has("band", "microsoft")
                   .toList();
        Assert.assertEquals(1, vertexes.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByNotEqualAll() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertexes = graph.traversal().V()
                                .has("cpu", "3.3GHz")
                                .has("band", "apple")
                                .has("ram", "16GB")
                                .toList();
        Assert.assertEquals(0, vertexes.size());

        vertexes = graph.traversal().V()
                   .has("ram", "32GB")
                   .has("cpu", "4.8GHz")
                   .has("band", "microsoft")
                   .toList();
        Assert.assertEquals(0, vertexes.size());
    }

    private void init10Vertices() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "James Gosling", "age", 62,
                        "lived", "Canadian");
        graph.addVertex(T.label, "author", "id", 2,
                        "name", "Guido van Rossum", "age", 61,
                        "lived", "California");

        graph.addVertex(T.label, "language", "name", "java");
        graph.addVertex(T.label, "language", "name", "c++");
        graph.addVertex(T.label, "language", "name", "python",
                        "dynamic", true);

        graph.addVertex(T.label, "book", "name", "java-1");
        graph.addVertex(T.label, "book", "name", "java-2");
        graph.addVertex(T.label, "book", "name", "java-3");
        graph.addVertex(T.label, "book", "name", "java-4");
        graph.addVertex(T.label, "book", "name", "java-5");

        graph.tx().commit();
    }

    private void init5Persons() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3);
        graph.addVertex(T.label, "person", "name", "James",
                        "city", "Beijing", "age", 19);
        graph.addVertex(T.label, "person", "name", "Tom Cat",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Lisa",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Hebe",
                        "city", "Taipei", "age", 21);

        graph.tx().commit();
    }

    private void init5Computers() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "computer", "name", "YangTian T6900C",
                        "band", "lenovo", "cpu", "3.2GHz", "ram", "8GB",
                        "price", 4599);
        graph.addVertex(T.label, "computer", "name", "Fengxing K450e",
                        "band", "lenovo", "cpu", "3.2GHz", "ram", "16GB",
                        "price", 6099);
        graph.addVertex(T.label, "computer", "name", "iMac MK482CH/A",
                        "band", "apple", "cpu", "3.3GHz", "ram", "32GB",
                        "price", 15990);
        graph.addVertex(T.label, "computer", "name", "Surface Studio",
                        "band", "microsoft", "cpu", "4.6GHz", "ram", "32GB",
                        "price", 35990);
        graph.addVertex(T.label, "computer", "name", "Zen AIO Pro",
                        "band", "asus", "cpu", "3.2GHz", "ram", "16GB",
                        "price", 6999);

        graph.tx().commit();
    }

    private Vertex vertex(String label, String pkName, Object pkValue) {
        List<Vertex> vertexes = graph().traversal().V()
                                .hasLabel(label).has(pkName, pkValue)
                                .toList();
        Assert.assertTrue(vertexes.size() <= 1);
        return vertexes.size() == 1 ? vertexes.get(0) : null;
    }

    private static void assertContains(List<Vertex> vertices,
                                       Object... keyValues) {
        Assert.assertTrue(Utils.contains(vertices,
                          new FakeVertex(keyValues)));
    }

    private static void assertNotContains(List<Vertex> vertices,
                                          Object... keyValues) {
        Assert.assertFalse(Utils.contains(vertices,
                           new FakeVertex(keyValues)));
    }
}
