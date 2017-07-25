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

package com.baidu.hugegraph.type.schema;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;


public abstract class PropertyKey extends SchemaElement {

    public PropertyKey(String name) {
        super(name);
    }

    @Override
    public HugeType type() {
        return HugeType.PROPERTY_KEY;
    }

    @Override
    public PropertyKey properties(String... propertyNames) {
        this.properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    public Class<?> clazz() {
        Class<?> dataType = this.dataType().clazz();
        Class<?> cls = null;

        switch (this.cardinality()) {
            case SINGLE:
                cls = dataType;
                break;
            // A set of values: Set<DataType>
            case SET:
                cls = LinkedHashSet.class;
                break;
            // A list of values: List<DataType>
            case LIST:
                cls = LinkedList.class;
                break;
            default:
                assert false;
                break;
        }
        return cls;
    }

    // Check type of the value valid
    public <V> boolean checkDataType(V value) {
        return this.dataType().clazz().isInstance(value);
    }

    // Check type of all the values(may be some of list properties) valid
    public <V> boolean checkDataType(Collection<V> values) {
        boolean valid = true;
        for (Object o : values) {
            if (!this.checkDataType(o)) {
                valid = false;
                break;
            }
        }
        return valid;
    }

    // Check property value valid
    public <V> boolean checkValue(V value) {
        boolean valid = false;

        switch (this.cardinality()) {
            case SINGLE:
                valid = this.checkDataType(value);
                break;
            case SET:
                valid = value instanceof Set;
                valid = valid && this.checkDataType((Set<?>) value);
                break;
            case LIST:
                valid = value instanceof List;
                valid = valid && this.checkDataType((List<?>) value);
                break;
            default:
                assert false;
                break;
        }
        return valid;
    }

    public abstract DataType dataType();

    public abstract Cardinality cardinality();

    public abstract PropertyKey asText();

    public abstract PropertyKey asInt();

    public abstract PropertyKey asTimestamp();

    public abstract PropertyKey asUuid();

    public abstract PropertyKey asBoolean();

    public abstract PropertyKey asByte();

    public abstract PropertyKey asBlob();

    public abstract PropertyKey asDouble();

    public abstract PropertyKey asFloat();

    public abstract PropertyKey asLong();

    public abstract PropertyKey valueSingle();

    public abstract PropertyKey valueList();

    public abstract PropertyKey valueSet();

    @Override
    public PropertyKey ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public abstract PropertyKey create();

    @Override
    public abstract PropertyKey append();

    @Override
    public abstract PropertyKey eliminate();

    @Override
    public abstract void remove();
}
