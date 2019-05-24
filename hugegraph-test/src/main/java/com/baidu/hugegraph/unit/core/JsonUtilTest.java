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

package com.baidu.hugegraph.unit.core;

import java.util.Arrays;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeEdgeProperty;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Utils;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.unit.FakeObjects;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableMap;

public class JsonUtilTest extends BaseUnitTest {

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testSerializeStringId() {
        Id id = IdGenerator.of("123456");
        String json = JsonUtil.toJson(id);
        Assert.assertEquals("\"123456\"", json);
    }

    @Test
    public void testSerializeLongId() {
        Id id = IdGenerator.of(123456L);
        String json = JsonUtil.toJson(id);
        Assert.assertEquals("123456", json);
    }

    @Test
    public void testSerializePropertyKey() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        String json = JsonUtil.toJson(name);
        Assert.assertEquals("{\"id\":1,\"name\":\"name\"," +
                            "\"data_type\":\"TEXT\"," +
                            "\"cardinality\":\"SINGLE\"," +
                            "\"properties\":[],\"user_data\":{}}", json);

        PropertyKey rate = fakeObject.newPropertyKey(IdGenerator.of(2), "rate",
                                                     DataType.INT,
                                                     Cardinality.LIST);
        json = JsonUtil.toJson(rate);
        Assert.assertEquals("{\"id\":2,\"name\":\"rate\"," +
                            "\"data_type\":\"INT\",\"cardinality\":\"LIST\"," +
                            "\"properties\":[],\"user_data\":{}}", json);
    }

    @Test
    public void testSerializeVertexLabel() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                     DataType.INT,
                                                     Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());
        Mockito.when(fakeObject.graph().mapPkId2Name(vl.properties()))
               .thenReturn(Arrays.asList(name.name(), age.name(), city.name()));

        String json = JsonUtil.toJson(vl);
        Assert.assertEquals("{\"id\":1,\"name\":\"person\"," +
                            "\"id_strategy\":\"CUSTOMIZE_NUMBER\"," +
                            "\"primary_keys\":[],\"nullable_keys\":[]," +
                            "\"index_labels\":[]," +
                            "\"properties\":[\"name\",\"age\",\"city\"]," +
                            "\"enable_label_index\":true,\"user_data\":{}}",
                            json);
    }

    @Test
    public void testSerializeEdgeLabel() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                    DataType.INT,
                                                    Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");
        PropertyKey date = fakeObject.newPropertyKey(IdGenerator.of(4), "date",
                                                     DataType.DATE);
        PropertyKey weight = fakeObject.newPropertyKey(IdGenerator.of(5),
                                                       "weight",
                                                       DataType.DOUBLE);

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());

        EdgeLabel el = fakeObject.newEdgeLabel(IdGenerator.of(1), "knows",
                                               Frequency.SINGLE,
                                               vl.id(), vl.id(),
                                               date.id(), weight.id());

        Mockito.when(fakeObject.graph().vertexLabel(vl.id())).thenReturn(vl);
        Mockito.when(fakeObject.graph().mapPkId2Name(el.properties()))
               .thenReturn(Arrays.asList(date.name(), weight.name()));

        String json = JsonUtil.toJson(el);
        Assert.assertEquals("{\"id\":1,\"name\":\"knows\"," +
                            "\"source_label\":\"person\"," +
                            "\"target_label\":\"person\"," +
                            "\"frequency\":\"SINGLE\",\"sort_keys\":[]," +
                            "\"nullable_keys\":[],\"index_labels\":[]," +
                            "\"properties\":[\"date\",\"weight\"]," +
                            "\"enable_label_index\":true," +
                            "\"user_data\":{}}", json);
    }

    @Test
    public void testSerializeIndexLabel() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                    DataType.INT,
                                                    Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());

        IndexLabel il = fakeObject.newIndexLabel(IdGenerator.of(1),
                                                 "personByAgeAndCity",
                                                 HugeType.VERTEX_LABEL,
                                                 vl.id(),
                                                 IndexType.SECONDARY,
                                                 age.id(), city.id());

        Mockito.when(fakeObject.graph().vertexLabel(vl.id())).thenReturn(vl);
        Mockito.when(fakeObject.graph().mapPkId2Name(il.indexFields()))
               .thenReturn(Arrays.asList(age.name(), city.name()));

        String json = JsonUtil.toJson(il);
        Assert.assertEquals("{\"id\":1," +
                            "\"name\":\"personByAgeAndCity\"," +
                            "\"base_type\":\"VERTEX_LABEL\"," +
                            "\"base_value\":\"person\"," +
                            "\"index_type\":\"SECONDARY\"," +
                            "\"fields\":[\"age\",\"city\"]}", json);
    }

    @Test
    public void testSerializeEdgeId() {
        Id id = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                           IdGenerator.of(1), "",
                           IdGenerator.of("1:josh"));
        String json = JsonUtil.toJson(id);
        Assert.assertEquals("\"S1:marko>1>>S1:josh\"", json);
    }

    @Test
    public void testSerializeVertexWithNumberId() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                    DataType.INT,
                                                    Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());

        Id id = IdGenerator.of(123456L);
        HugeVertex vertex = new HugeVertex(fakeObject.graph(), id, vl);

        Map<Id, HugeProperty<?>> properties = ImmutableMap.of(
                name.id(), new HugeVertexProperty<>(vertex, name, "marko"),
                age.id(), new HugeVertexProperty<>(vertex, age, 29),
                city.id(), new HugeVertexProperty<>(vertex, city, "Beijing")
        );
        Whitebox.setInternalState(vertex, "properties", properties);

        String json = JsonUtil.toJson(vertex);
        Assert.assertEquals("{\"id\":123456,\"label\":\"person\"," +
                            "\"type\":\"vertex\",\"properties\":{\"" +
                            "name\":\"marko\",\"age\":29," +
                            "\"city\":\"Beijing\"}}", json);
    }

    @Test
    public void testSerializeEdge() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                    DataType.INT,
                                                    Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");
        PropertyKey date = fakeObject.newPropertyKey(IdGenerator.of(4), "date",
                                                     DataType.DATE);
        PropertyKey weight = fakeObject.newPropertyKey(IdGenerator.of(5),
                                                       "weight",
                                                       DataType.DOUBLE);

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());

        EdgeLabel el = fakeObject.newEdgeLabel(IdGenerator.of(1), "knows",
                                               Frequency.SINGLE,
                                               vl.id(), vl.id(),
                                               date.id(), weight.id());

        HugeVertex source = new HugeVertex(fakeObject.graph(),
                                           IdGenerator.of(123456), vl);
        HugeVertex target = new HugeVertex(fakeObject.graph(),
                                           IdGenerator.of(987654), vl);

        Id id = EdgeId.parse("L123456>1>>L987654");
        HugeEdge edge = new HugeEdge(fakeObject.graph(), id, el);
        Whitebox.setInternalState(edge, "sourceVertex", source);
        Whitebox.setInternalState(edge, "targetVertex", target);

        Map<Id, HugeProperty<?>> properties = ImmutableMap.of(
                date.id(), new HugeEdgeProperty<>(edge, date,
                                                  Utils.date("2019-03-12")),
                weight.id(), new HugeEdgeProperty<>(edge, weight, 0.8)
        );
        Whitebox.setInternalState(edge, "properties", properties);

        long dateTime = Utils.date("2019-03-12").getTime();
        String json = JsonUtil.toJson(edge);
        Assert.assertEquals(String.format("{\"id\":\"L123456>1>>L987654\"," +
                            "\"label\":\"knows\",\"type\":\"edge\"," +
                            "\"outV\":123456,\"outVLabel\":\"person\"," +
                            "\"inV\":987654,\"inVLabel\":\"person\"," +
                            "\"properties\":{\"date\":%s," +
                            "\"weight\":0.8}}", dateTime), json);
    }
}
