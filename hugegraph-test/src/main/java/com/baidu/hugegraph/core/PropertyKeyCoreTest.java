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

import org.junit.Test;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.google.common.collect.ImmutableList;

public class PropertyKeyCoreTest extends SchemaCoreTest {

    @Test
    public void testAddPropertyKey() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id")
                         .asText()
                         .valueSingle()
                         .create();

        Assert.assertEquals("id", id.name());
        Assert.assertEquals(DataType.TEXT, id.dataType());
        Assert.assertEquals(Cardinality.SINGLE, id.cardinality());
    }

    @Test
    public void testAddPropertyKeyWithValidName() {
        SchemaManager schema = graph().schema();

        // One space and single char
        schema.propertyKey(" s").create();
        schema.propertyKey("s s").create();

        schema.propertyKey(" .").create();
        schema.propertyKey(". .").create();

        schema.propertyKey("@$%^&*()_+`-={}|[]\"<?;'~,./\\").create();
        schema.propertyKey("azAZ0123456789").create();

        schema.propertyKey(" ~").create();
        schema.propertyKey("x~").create();
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
        // Multi spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("    ").create();
        });

        // End with spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("s ").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey(" . ").create();
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

        // Start with '~'
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("~").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("~ ").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("~x").create();
        });
    }

    @Test
    public void testAddPropertyKeyWithoutDataType() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id").valueSingle().create();
        Assert.assertEquals(DataType.TEXT, id.dataType());
    }

    @Test
    public void testAddPropertyKeyWithoutCardinality() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id").asText().create();
        Assert.assertEquals(Cardinality.SINGLE, id.cardinality());
    }

    @Test
    public void testAddPropertyKeyWithoutDataTypeAndCardinality() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id").create();
        Assert.assertEquals(DataType.TEXT, id.dataType());
        Assert.assertEquals(Cardinality.SINGLE, id.cardinality());
    }

    @Test
    public void testRemovePropertyKey() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("id").valueSingle().create();
        Assert.assertNotNull(schema.getPropertyKey("id"));

        schema.propertyKey("id").remove();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getPropertyKey("id");
        });
    }

    @Test
    public void testRemoveNotExistPropertyKey() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("not-exist-pk").remove();
    }

    @Test
    public void testRemovePropertyKeyUsedByVertexLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("name").remove();
        });
    }

    @Test
    public void testRemovePropertyKeyUsedByEdgeLabel() {
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

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("time").remove();
        });
    }

    @Test
    public void testAddPropertyKeyWithUserData() {
        SchemaManager schema = graph().schema();

        PropertyKey age = schema.propertyKey("age")
                                .userdata("min", 0)
                                .userdata("max", 100)
                                .create();
        Assert.assertEquals(2, age.userdata().size());
        Assert.assertEquals(0, age.userdata().get("min"));
        Assert.assertEquals(100, age.userdata().get("max"));

        PropertyKey id = schema.propertyKey("id")
                               .userdata("length", 15)
                               .userdata("length", 18)
                               .create();
        // The same key user data will be overwritten
        Assert.assertEquals(1, id.userdata().size());
        Assert.assertEquals(18, id.userdata().get("length"));

        PropertyKey sex = schema.propertyKey("sex")
                                .userdata("range",
                                          ImmutableList.of("male", "female"))
                                .create();
        Assert.assertEquals(1, sex.userdata().size());
        Assert.assertEquals(ImmutableList.of("male", "female"),
                            sex.userdata().get("range"));
    }

    @Test
    public void testAppendPropertyKeyWithUserData() {
        SchemaManager schema = graph().schema();

        PropertyKey age = schema.propertyKey("age")
                                .userdata("min", 0)
                                .create();
        Assert.assertEquals(1, age.userdata().size());
        Assert.assertEquals(0, age.userdata().get("min"));

        age = schema.propertyKey("age")
                    .userdata("min", 1)
                    .userdata("max", 100)
                    .append();
        Assert.assertEquals(2, age.userdata().size());
        Assert.assertEquals(1, age.userdata().get("min"));
        Assert.assertEquals(100, age.userdata().get("max"));
    }

    @Test
    public void testEliminatePropertyKeyWithUserData() {
        SchemaManager schema = graph().schema();

        PropertyKey age = schema.propertyKey("age")
                                .userdata("min", 0)
                                .userdata("max", 100)
                                .create();
        Assert.assertEquals(2, age.userdata().size());
        Assert.assertEquals(0, age.userdata().get("min"));
        Assert.assertEquals(100, age.userdata().get("max"));

        age = schema.propertyKey("age")
                    .userdata("max", "")
                    .eliminate();
        Assert.assertEquals(1, age.userdata().size());
        Assert.assertEquals(0, age.userdata().get("min"));
    }

    @Test
    public void testUpdatePropertyKeyWithNonUserData() {
        SchemaManager schema = graph().schema();

        schema.propertyKey("age")
              .asInt()
              .valueSingle()
              .userdata("min", 0)
              .create();

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("age").asLong().append();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("age").valueList().append();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("age").asLong().eliminate();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("age").valueList().eliminate();
        });
    }
}
