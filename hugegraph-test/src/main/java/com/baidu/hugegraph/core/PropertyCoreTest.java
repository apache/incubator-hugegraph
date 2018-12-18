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

import java.text.ParseException;
import java.util.Date;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.testutil.Utils;

public abstract class PropertyCoreTest extends BaseCoreTest {

    protected abstract <V> V property(String key, V value);

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("uid").asUuid().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("gender").asBoolean().create();
        schema.propertyKey("time").asDate().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueList().create();
        schema.propertyKey("contribution").asText().valueSet().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("weight").asDouble().create();
        schema.propertyKey("education").asByte().create();
        schema.propertyKey("amount").asLong().create();
        schema.propertyKey("height").asFloat().create();
        schema.propertyKey("img").asBlob().create();

        schema.vertexLabel("person")
              .properties("id", "name", "age", "city", "img", "gender",
                          "education", "amount", "height", "weight",
                          "time", "uid")
              .nullableKeys("name", "age", "city", "img", "gender",
                            "education", "amount", "height", "weight",
                            "time", "uid")
              .primaryKeys("id")
              .create();

        schema.edgeLabel("transfer")
              .properties("id", "name", "age", "city", "img", "gender",
                          "education", "amount", "height", "weight",
                          "time", "uid")
              .nullableKeys("name", "age", "city", "img", "gender",
                            "education", "amount", "height", "weight",
                            "time", "uid")
              .link("person", "person")
              .create();
    }

    public static class VertexPropertyCoreTest extends PropertyCoreTest {

        @Override
        protected <V> V property(String key, V value) {
            HugeGraph graph = graph();
            Vertex vertex = graph.addVertex(T.label, "person",
                                            "id", 1, key, value);
            graph.tx().commit();
            return graph.vertices(vertex.id()).next().value(key);
        }
    }

    public static class EdgePropertyCoreTest extends PropertyCoreTest {

        @Override
        protected <V> V property(String key, V value) {
            HugeGraph graph = graph();
            Vertex vertex1 = graph.addVertex(T.label, "person", "id", 1);
            Vertex vertex2 = graph.addVertex(T.label, "person", "id", 2);
            Edge edge = vertex1.addEdge("transfer", vertex2,
                                        "id", 1, key, value);
            graph.tx().commit();
            return graph.edges(edge.id()).next().value(key);
        }
    }

    @Test
    public void testTypeBoolean() {
        boolean gender = true;
        Assert.assertEquals(gender, property("gender", gender));

        gender = false;
        Assert.assertEquals(gender, property("gender", gender));
    }

    @Test
    public void testTypeByte() {
        Byte education = 3;
        Assert.assertEquals(education, property("education", education));
    }

    @Test
    public void testTypeInt() {
        Integer age = 18;
        Assert.assertEquals(age, property("age", age));
    }

    @Test
    public void testTypeLong() {
        Long amount = 18888888888L;
        Assert.assertEquals(amount, property("amount", amount));
    }

    @Test
    public void testTypeFloat() {
        Float height = 1.86F;
        Assert.assertEquals(height, property("height", height));
    }

    @Test
    public void testTypeDouble() {
        Double weight = 1.86;
        Assert.assertEquals(weight, property("weight", weight));
    }

    @Test
    public void testTypeString() {
        String name = "Jame";
        Assert.assertEquals(name, property("name", name));
    }

    @Test
    public void testTypeBlob() {
        byte[] img = new byte[]{1, 2, 8, 50, 80, 96, 110, 125, -1, -10, -100};
        Assert.assertArrayEquals(img, property("img", img));
    }

    @Test
    public void testTypeDate() throws ParseException {
        Date time = new Date();
        Assert.assertEquals(time, property("time", time));

        Date expected = Utils.date("2018-12-12 00:00:00.000");
        String date = "2018-12-12";
        Assert.assertEquals(expected, property("time", date));
        date = "2018-12-12 00:00:00";
        Assert.assertEquals(expected, property("time", date));
        date = "2018-12-12 00:00:00.000";
        Assert.assertEquals(expected, property("time", date));
    }

    @Test
    public void testTypeUuid() {
        UUID uid = UUID.randomUUID();
        Assert.assertEquals(uid, property("uid", uid));
    }
}
