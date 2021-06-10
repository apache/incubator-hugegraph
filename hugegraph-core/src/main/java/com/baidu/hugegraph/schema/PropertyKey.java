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
import com.baidu.hugegraph.type.define.AggregateType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.ReadFrequency;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LongEncoding;

public class PropertyKey extends SchemaElement implements Propfiable {

    private DataType dataType;
    private Cardinality cardinality;
    private AggregateType aggregateType;
    private ReadFrequency readFrequency;

    public PropertyKey(final HugeGraph graph, Id id, String name) {
        super(graph, id, name);
        this.dataType = DataType.TEXT;
        this.cardinality = Cardinality.SINGLE;
        this.aggregateType = AggregateType.NONE;
        this.readFrequency = ReadFrequency.OLTP;
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

    public AggregateType aggregateType() {
        return this.aggregateType;
    }

    public void aggregateType(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
    }

    public void readFrequency(ReadFrequency readFrequency) {
        this.readFrequency = readFrequency;
    }

    public ReadFrequency readFrequency() {
        return this.readFrequency;
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

    public void defineDefaultValue(Object value) {
        // TODO add a field default_value
        this.userdata().put(Userdata.DEFAULT_VALUE, value);
    }

    public Object defaultValue() {
        // TODO add a field default_value
        return this.userdata().get(Userdata.DEFAULT_VALUE);
    }

    public boolean hasSameContent(PropertyKey other) {
        return super.hasSameContent(other) &&
               this.dataType == other.dataType() &&
               this.cardinality == other.cardinality() &&
               this.aggregateType == other.aggregateType() &&
               this.readFrequency == other.readFrequency();
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
        switch (this.cardinality) {
            case SET:
                return (T) new LinkedHashSet<>();
            case LIST:
                return (T) new ArrayList<>();
            default:
                // pass
                break;
        }

        try {
            return (T) this.implementClazz().newInstance();
        } catch (Exception e) {
            throw new HugeException("Failed to new instance of %s: %s",
                                    this.implementClazz(), e.toString());
        }
    }

    /**
     * Check property value valid
     * @param value the property value to be checked data type and cardinality
     * @param <V> the property value class
     * @return true if data type and cardinality satisfy requirements,
     *         otherwise false
     */
    public <V> boolean checkValueType(V value) {
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

    /**
     * Check type of the value valid
     * @param value the property value to be checked data type
     * @param <V>   the property value original data type
     * @return true if the value is or can convert to the data type,
     *         otherwise false
     */
    private <V> boolean checkDataType(V value) {
        return this.dataType().clazz().isInstance(value);
    }

    /**
     * Check type of all the values(may be some of list properties) valid
     * @param values the property values to be checked data type
     * @param <V> the property value class
     * @return true if all the values are or can convert to the data type,
     *         otherwise false
     */
    private <V> boolean checkDataType(Collection<V> values) {
        boolean valid = true;
        for (Object o : values) {
            if (!this.checkDataType(o)) {
                valid = false;
                break;
            }
        }
        return valid;
    }

    public <V> Object serialValue(V value, boolean encodeNumber) {
        V validValue = this.validValue(value);
        E.checkArgument(validValue != null,
                        "Invalid property value '%s' for key '%s'",
                        value, this.name());
        E.checkArgument(this.cardinality.single(),
                        "The cardinality can't be '%s' for navigation key '%s'",
                        this.cardinality, this.name());
        if (encodeNumber &&
            (this.dataType.isNumber() || this.dataType.isDate())) {
            return LongEncoding.encodeNumber(validValue);
        }
        return validValue;
    }

    public <V> V validValueOrThrow(V value) {
        V validValue = this.validValue(value);
        if (validValue == null) {
            E.checkArgument(false,
                            "Invalid property value '%s' for key '%s', " +
                            "expect a value of type %s, actual type %s",
                            value, this.name(), this.clazz(),
                            value.getClass().getSimpleName());
        }
        return validValue;
    }

    public <V> V validValue(V value) {
        try {
            return this.convValue(value);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(String.format(
                      "Invalid property value '%s' for key '%s': %s",
                      value, this.name(), e.getMessage()));
        }
    }

    @SuppressWarnings("unchecked")
    private <V, T> V convValue(V value) {
        if (value == null) {
            return null;
        }
        if (this.checkValueType(value)) {
            // Same as expected type, no conversion required
            return value;
        }

        V validValue = null;
        Collection<T> validValues;
        if (this.cardinality.single()) {
            validValue = this.convSingleValue(value);
        } else if (value instanceof Collection) {
            assert this.cardinality.multiple();
            Collection<T> collection = (Collection<T>) value;
            if (value instanceof Set) {
                validValues = new LinkedHashSet<>(collection.size());
            } else {
                assert value instanceof List;
                validValues = new ArrayList<>(collection.size());
            }
            for (T element : collection) {
                element = this.convSingleValue(element);
                if (element == null) {
                    validValues = null;
                    break;
                }
                validValues.add(element);
            }
            validValue = (V) validValues;
        } else {
            assert this.cardinality.multiple();
            E.checkArgument(false,
                            "Property value must be %s, but got '%s'(%s)",
                            this.cardinality, value,
                            value.getClass().getSimpleName());
        }
        return validValue;
    }

    private <V> V convSingleValue(V value) {
        if (value == null) {
            return null;
        }
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
        } else if (this.dataType().isBlob()) {
            @SuppressWarnings("unchecked")
            V blob = (V) this.dataType().valueToBlob(value);
            return blob;
        }

        if (this.checkDataType(value)) {
            return value;
        }
        return null;
    }

    public interface Builder extends SchemaBuilder<PropertyKey> {

        PropertyKeyWithTask createWithTask();

        Builder asText();

        Builder asInt();

        Builder asDate();

        Builder asUUID();

        Builder asBoolean();

        Builder asByte();

        Builder asBlob();

        Builder asDouble();

        Builder asFloat();

        Builder asLong();

        Builder valueSingle();

        Builder valueList();

        Builder valueSet();

        Builder calcMax();

        Builder calcMin();

        Builder calcSum();

        Builder calcOld();

        Builder calcSet();

        Builder calcList();

        Builder readFrequency(ReadFrequency readFrequency);

        Builder cardinality(Cardinality cardinality);

        Builder dataType(DataType dataType);

        Builder aggregateType(AggregateType aggregateType);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);
    }


    public static class PropertyKeyWithTask {

        private PropertyKey propertyKey;
        private Id task;

        public PropertyKeyWithTask(PropertyKey propertyKey, Id task) {
            E.checkNotNull(propertyKey, "property key");
            this.propertyKey = propertyKey;
            this.task = task;
        }

        public void propertyKey(PropertyKey propertyKey) {
            E.checkNotNull(propertyKey, "property key");
            this.propertyKey = propertyKey;
        }

        public PropertyKey propertyKey() {
            return this.propertyKey;
        }

        public void task(Id task) {
            this.task = task;
        }

        public Id task() {
            return this.task;
        }
    }
}
