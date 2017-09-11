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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.core.FakeObjects.FakeVertex;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Shard;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class VertexCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        LOG.info("===============  propertyKey  ================");

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

        LOG.info("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("computer")
              .properties("name", "band", "cpu", "ram", "price")
              .primaryKeys("name", "band")
              .ifNotExist()
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id")
              .nullableKeys("name", "age", "lived")
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

        LOG.info("===============  vertexLabel index  ================");

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

        // Directly into the back-end
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
        vertex = vertex("review", "id", 1);
        Assert.assertEquals(ImmutableSet.of("+1", "+2"),
                            vertex.value("contribution"));

        vertex = graph.addVertex(T.label, "review", "id", 2,
                                 "contribution",
                                 ImmutableSet.of("+1", "+1", "+2"));
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
    public void testAddVertexWithNotExistsProp() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book", "not-exists-porp", "test");
        });
    }

    @Test
    public void testAddVertexWithNotExistsVertexProp() {
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

        // Use tinkerpop tx
        graph.tx().open();
        graph.addVertex(T.label, "book", "name", "java-4");
        graph.addVertex(T.label, "book", "name", "java-5");
        graph.tx().close();

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

    @Test()
    public void testQueryByIdNotFound() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query vertex by id which not exists
        Id id = SplicingIdGenerator.splicing("author", "not-exists-id");
        Assert.assertTrue(graph.traversal().V(id).toList().isEmpty());
        Assert.assertThrows(NotFoundException.class, () -> {
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
                                .hasLabel("author").has("id", "1").toList();
        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");
    }

    @Test
    public void testQueryFilterByPropName() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query vertex by condition (filter by property name)
        ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
        q.eq(HugeKeys.LABEL, "language");
        q.key(HugeKeys.PROPERTIES, "dynamic");
        List<Vertex> vertexes = ImmutableList.copyOf(graph.vertices(q));

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                       T.label, "language", "name", "python", "dynamic", true);
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

        vertexes = graph.traversal().V().toList();
        Assert.assertEquals(9, vertexes.size());
        assertNotContains(vertexes,
                          T.label, "author", "id", 1, "name", "James Gosling",
                          "age", 62, "lived", "Canadian");
    }

    @Test
    public void testRemoveVertexNotExists() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertexes = graph.traversal().V().toList();
        Assert.assertEquals(10, vertexes.size());
        assertContains(vertexes,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        Vertex vertex = vertex("author", "id", 1);
        vertex.remove();

        vertexes = graph.traversal().V().toList();
        Assert.assertEquals(9, vertexes.size());
        assertNotContains(vertexes,
                          T.label, "author", "id", 1, "name", "James Gosling",
                          "age", 62, "lived", "Canadian");

        // Remove again
        vertex.remove();
    }

    @Test
    public void testRemoveVertexOneByOne() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertexes = graph.traversal().V().toList();
        Assert.assertEquals(10, vertexes.size());

        for (int i = 0; i < vertexes.size(); i++) {
            vertexes.get(i).remove();
            Assert.assertEquals(9 - i, graph.traversal().V().toList().size());
        }
    }

    @Test
    public void testAddVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1);

        // Add properties
        vertex.property("name", "Tom");
        vertex.property("age", 10);
        vertex.property("lived", "USA");

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tom", vertex.property("name").value());
        Assert.assertEquals(10, vertex.property("age").value());
        Assert.assertEquals("USA", vertex.property("lived").value());
    }

    @Test
    public void testAddVertexPropertyExisted() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("name", "Tom2");
        });

        vertex.property("age", 10);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("age", "11");
        });
    }

    @Test
    public void testAddVertexPropertyNotInVertexLabel() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("time", "2017-1-1");
        });
    }

    @Test
    public void testAddVertexPropertyWithNotExistPropKey() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("prop-not-exist", "2017-1-1");
        });
    }

    @Test
    public void testRemoveVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "age", 10,
                                        "lived", "USA");

        // Remove "name" property
        Assert.assertTrue(vertex.property("name").isPresent());
        Assert.assertTrue(vertex.property("lived").isPresent());
        vertex.property("name").remove();
        Assert.assertFalse(vertex.property("name").isPresent());
        Assert.assertTrue(vertex.property("lived").isPresent());

        // Remove "lived" property
        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("name").isPresent());
        Assert.assertTrue(vertex.property("lived").isPresent());
        vertex.property("lived").remove();
        Assert.assertFalse(vertex.property("name").isPresent());
        Assert.assertFalse(vertex.property("lived").isPresent());

        vertex = vertex("author", "id", 1);
        Assert.assertEquals(10, vertex.property("age").value());
        Assert.assertFalse(vertex.property("name").isPresent());
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
    public void testRemoveVertexPropertyWithIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        // TODO: this should be resolved
        Assert.assertThrows(IllegalStateException.class, () -> {
            vertex.property("age").remove();
        });
    }

    @Test
    public void testUpdateVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom");
        vertex.property("name").remove();
        vertex.property("name", "Tom-2");

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tom-2", vertex.property("name").value());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testScanVertex() {
        HugeGraph graph = graph();
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
        graph.tx().open();

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

        graph.tx().close();
    }

    private void init5Persons() {
        HugeGraph graph = graph();
        graph.tx().open();

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

        graph.tx().close();
    }

    private void init5Computers() {
        HugeGraph graph = graph();
        graph.tx().open();

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

        graph.tx().close();
    }

    private Vertex vertex(String label, String pkName, Object pkValue) {
        List<Vertex> vertexes = graph().traversal().V()
                                .hasLabel(label).has(pkName, pkValue)
                                .toList();
        Assert.assertEquals(1, vertexes.size());
        return vertexes.get(0);
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
