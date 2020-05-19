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

package com.baidu.hugegraph.unit.serializer;

import org.junit.Test;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.serializer.BinaryScatterSerializer;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.serializer.TextSerializer;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;

public class SerializerFactoryTest extends BaseUnitTest {

    @Test
    public void testSerializer() {
        AbstractSerializer serializer = SerializerFactory.serializer("text");
        Assert.assertEquals(TextSerializer.class, serializer.getClass());

        serializer = SerializerFactory.serializer("binary");
        Assert.assertEquals(BinarySerializer.class, serializer.getClass());

        serializer = SerializerFactory.serializer("binaryscatter");
        Assert.assertEquals(BinaryScatterSerializer.class,
                            serializer.getClass());

        Assert.assertThrows(BackendException.class, () -> {
            SerializerFactory.serializer("invalid");
        }, e -> {
            Assert.assertTrue(e.getMessage().contains(
                              "Not exists serializer:"));
        });
    }

    @Test
    public void testRegister() {
        SerializerFactory.register("fake", FakeSerializer.class.getName());
        Assert.assertEquals(FakeSerializer.class,
                            SerializerFactory.serializer("fake").getClass());

        Assert.assertThrows(BackendException.class, () -> {
            // exist
            SerializerFactory.register("fake", FakeSerializer.class.getName());
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("Exists serializer:"));
        });

        Assert.assertThrows(BackendException.class, () -> {
            // invalid class
            SerializerFactory.register("fake", "com.baidu.hugegraph.Invalid");
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("Invalid class:"));
        });

        Assert.assertThrows(BackendException.class, () -> {
            // subclass
            SerializerFactory.register("fake", "com.baidu.hugegraph.HugeGraph");
        }, e -> {
            Assert.assertTrue(e.getMessage().contains(
                              "Class is not a subclass of class"));
        });
    }

    public static class FakeSerializer extends BinarySerializer {

        public FakeSerializer() {
            super(true, true);
        }
    }
}
