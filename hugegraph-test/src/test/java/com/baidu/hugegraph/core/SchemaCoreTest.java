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

import java.util.Collection;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.testutil.Assert;

public class SchemaCoreTest extends BaseCoreTest {

    /**
     * Utils method to init some property keys
     */
    protected void initPropertyKeys() {
        SchemaManager schema = graph().schema();

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("contribution").asText().create();
        schema.propertyKey("weight").asDouble().create();
    }

    protected void assertVLEqual(String label, Id id) {
        VertexLabel vertexLabel = graph().vertexLabel(label);
        Assert.assertEquals(id, vertexLabel.id());
    }

    protected void assertELEqual(String label, Id id) {
        EdgeLabel edgeLabel = graph().edgeLabel(label);
        Assert.assertEquals(id, edgeLabel.id());
    }

    protected void assertContainsPk(Collection<Id> ids, String... keys) {
        for (String key : keys) {
            PropertyKey pkey = graph().propertyKey(key);
            Assert.assertTrue(ids.contains(pkey.id()));
        }
    }

    protected void assertNotContainsPk(Collection<Id> ids, String... keys) {
        for (String key : keys) {
            Assert.assertNull(graph().schemaTransaction().getPropertyKey(key));
        }
    }

    protected void assertContainsIl(Collection<Id> ids, String... labels) {
        for (String label : labels) {
            IndexLabel indexLabel = graph().indexLabel(label);
            Assert.assertTrue(ids.contains(indexLabel.id()));
        }
    }

    protected void assertNotContainsIl(Collection<Id> ids, String... labels) {
        for (String label : labels) {
            Assert.assertNull(graph().schemaTransaction().getIndexLabel(label));
        }
    }
}
