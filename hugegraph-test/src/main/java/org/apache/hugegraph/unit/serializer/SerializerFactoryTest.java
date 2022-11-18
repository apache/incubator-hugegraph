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

package org.apache.hugegraph.unit.serializer;

import org.junit.Test;

import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.serializer.AbstractSerializer;
import org.apache.hugegraph.backend.serializer.BinaryScatterSerializer;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.backend.serializer.SerializerFactory;
import org.apache.hugegraph.backend.serializer.TextSerializer;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;

public class SerializerFactoryTest extends BaseUnitTest {

    @Test
    public void testSerializer() {
        HugeConfig config = FakeObjects.newConfig();
        AbstractSerializer serializer = SerializerFactory.serializer(config,"text");
        Assert.assertEquals(TextSerializer.class, serializer.getClass());

        serializer = SerializerFactory.serializer(config, "binary");
        Assert.assertEquals(BinarySerializer.class, serializer.getClass());

        serializer = SerializerFactory.serializer(config, "binaryscatter");
        Assert.assertEquals(BinaryScatterSerializer.class,
                            serializer.getClass());

        Assert.assertThrows(BackendException.class, () -> {
            SerializerFactory.serializer(config, "invalid");
        }, e -> {
            Assert.assertContains("Not exists serializer:", e.getMessage());
        });
    }

    @Test
    public void testRegister() {
        HugeConfig config = FakeObjects.newConfig();
        SerializerFactory.register("fake", FakeSerializer.class.getName());
        Assert.assertEquals(FakeSerializer.class,
                            SerializerFactory.serializer(config, "fake").getClass());

        Assert.assertThrows(BackendException.class, () -> {
            // exist
            SerializerFactory.register("fake", FakeSerializer.class.getName());
        }, e -> {
            Assert.assertContains("Exists serializer:", e.getMessage());
        });

        Assert.assertThrows(BackendException.class, () -> {
            // invalid class
            SerializerFactory.register("fake", "org.apache.hugegraph.Invalid");
        }, e -> {
            Assert.assertContains("Invalid class:", e.getMessage());
        });

        Assert.assertThrows(BackendException.class, () -> {
            // subclass
            SerializerFactory.register("fake", "org.apache.hugegraph.HugeGraph");
        }, e -> {
            Assert.assertContains("Class is not a subclass of class",
                                  e.getMessage());
        });
    }

    public static class FakeSerializer extends BinarySerializer {

        public FakeSerializer(HugeConfig config) {
            super(config);
        }

        public FakeSerializer() {
            super(true, true, false);
        }
    }
}
