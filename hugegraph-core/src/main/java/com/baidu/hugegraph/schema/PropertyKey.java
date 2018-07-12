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

package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.builder.SchemaBuilder;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;

public class PropertyKey extends SchemaElement {

    private DataType dataType;
    private Cardinality cardinality;

    public PropertyKey(final HugeGraph graph, Id id, String name) {
        super(graph, id, name);
        this.dataType = DataType.TEXT;
        this.cardinality = Cardinality.SINGLE;
    }

    @Override
    public HugeType type() {
        return HugeType.PROPERTY_KEY;
    }

    public PropertyKey properties(Id... properties) {
        this.properties.addAll(Arrays.asList(properties));
        return this;
    }

    public Class<?> clazz() {
        Class<?> cls;
        switch (this.cardinality) {
            case SINGLE:
                cls = this.dataType().clazz();
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
                throw new AssertionError(String.format(
                          "Unsupported cardinality: '%s'", this.cardinality));
        }
        return cls;
    }

    /**
     * Check type of the value valid
     * @param value the property value to be checked data type
     * @return true if the value is or can convert to the data type,
     *         otherwise false
     */
    public <V> boolean checkDataType(V value) {
        if (value instanceof Number) {
            return this.dataType().valueToNumber(value) != null;
        }
        return this.dataType().clazz().isInstance(value);
    }

    /**
     * Check type of all the values(may be some of list properties) valid
     * @param values the property values to be checked data type
     * @param <V> the property value class
     * @return true if all the values are or can convert to the data type,
     *         otherwise false
     */
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

    /**
     * Check property value valid
     * @param value the property value to be checked data type and cardinality
     * @param <V> the property value class
     * @return true if data type and cardinality satisfy requirements,
     *         otherwise false
     */
    public <V> boolean checkValue(V value) {
        boolean valid;

        switch (this.cardinality) {
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
                throw new AssertionError(String.format(
                          "Unsupported cardinality: '%s'", this.cardinality));
        }
        return valid;
    }

    public <V> V validValue(V value) {
        if (value == null) {
            return null;
        }

        if (this.cardinality == Cardinality.SINGLE) {
            if (this.dataType().isNumber()) {
                @SuppressWarnings("unchecked")
                V number = (V) this.dataType().valueToNumber(value);
                return number;
            } else if (this.dataType().isDate()) {
                @SuppressWarnings("unchecked")
                V date = (V) this.dataType().valueToDate(value);
                return date;
            } else if (this.dataType().isUUID()) {
                @SuppressWarnings("unchecked")
                V uuid = (V) this.dataType().valueToUUID(value);
                return uuid;
            }
        }

        if (this.checkValue(value)) {
            return value;
        }

        return null;
    }

    public DataType dataType() {
        return this.dataType;
    }

    public void dataType(DataType dataType) {
        this.dataType = dataType;
    }

    public Cardinality cardinality() {
        return this.cardinality;
    }

    public void cardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    public interface Builder extends SchemaBuilder<PropertyKey> {

        Builder asText();

        Builder asInt();

        Builder asDate();

        Builder asUuid();

        Builder asBoolean();

        Builder asByte();

        Builder asBlob();

        Builder asDouble();

        Builder asFloat();

        Builder asLong();

        Builder valueSingle();

        Builder valueList();

        Builder valueSet();

        Builder cardinality(Cardinality cardinality);

        Builder dataType(DataType dataType);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);
    }
}
