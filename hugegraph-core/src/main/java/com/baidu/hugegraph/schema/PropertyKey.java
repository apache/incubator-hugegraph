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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.builder.SchemaBuilder;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Propfiable;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LongEncoding;

public class PropertyKey extends SchemaElement implements Propfiable {

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

    @Override
    public Set<Id> properties() {
        return Collections.emptySet();
    }

    public PropertyKey properties(Id... properties) {
        if (properties.length > 0) {
            throw new NotSupportException("PropertyKey.properties(Id)");
        }
        return this;
    }

    public String clazz() {
        String dataType = this.dataType().clazz().getSimpleName();
        switch (this.cardinality) {
            case SINGLE:
                return dataType;
            // A set of values: Set<DataType>
            case SET:
                return String.format("Set<%s>", dataType);
            // A list of values: List<DataType>
            case LIST:
                return String.format("List<%s>", dataType);
            default:
                throw new AssertionError(String.format(
                          "Unsupported cardinality: '%s'", this.cardinality));
        }
    }

    public Class<?> implementClazz() {
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
                cls = ArrayList.class;
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported cardinality: '%s'", this.cardinality));
        }
        return cls;
    }

    @SuppressWarnings("unchecked")
    public <T> T newValue() {
        try {
            return (T) this.implementClazz().newInstance();
        } catch (Exception e) {
            throw new HugeException("Failed to new instance of %s: %s",
                                    this.implementClazz(), e.toString());
        }
    }

    /**
     * Check type of the value valid
     * @param value the property value to be checked data type
     * @param <V>   the property value original data type
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

    public <V> Object serialValue(V value) {
        V validValue = this.validValue(value);
        E.checkArgument(validValue != null,
                        "Invalid property value '%s' for key '%s'",
                        value, this.name());
        if (this.dataType.isNumber() || this.dataType.isDate()) {
            return LongEncoding.encodeNumber(validValue);
        }
        return validValue;
    }

    public <V> V validValueOrThrow(V value) {
        V validValue = this.validValue(value);
        E.checkArgument(validValue != null,
                        "Invalid property value '%s' for key '%s', " +
                        "expect a value of type %s, actual type %s",
                        value, this.name(), this.clazz(),
                        value.getClass().getSimpleName());
        return validValue;
    }

    public <V> V validValue(V value) {
        return this.convValue(value, true);
    }

    @SuppressWarnings("unchecked")
    public <V, T> V convValue(V value, boolean checkValue) {
        if (value == null) {
            return null;
        }

        V validValue;
        Collection<T> validValues;
        if (!(value instanceof Collection)) {
            validValue = this.convSingleValue(value);
        } else {
            if (value instanceof Set) {
                validValues = new HashSet<>();
            } else {
                E.checkArgument(value instanceof List,
                                "Property value must be Single, Set, List, " +
                                "but got %s", value);
                validValues = new ArrayList<>();
            }
            for (T element : (Collection<T>) value) {
                element = this.convSingleValue(element);
                if (element == null) {
                    return null;
                }
                validValues.add(element);
            }
            validValue = (V) validValues;
        }

        if (!checkValue || this.checkValue(validValue)) {
            return validValue;
        }
        return null;
    }

    private <V> V convSingleValue(V value) {
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
        return value;
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
