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
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.schema.builder.PropertyKeyBuilder;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringUtil;

public class PropertyKey extends SchemaElement {

    private DataType dataType;
    private Cardinality cardinality;

    public PropertyKey(String name) {
        super(name);
        this.dataType = DataType.TEXT;
        this.cardinality = Cardinality.SINGLE;
    }

    @Override
    public HugeType type() {
        return HugeType.PROPERTY_KEY;
    }

    public PropertyKey properties(String... propertyNames) {
        this.properties.addAll(Arrays.asList(propertyNames));
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
     */
    public <V> boolean checkDataType(V value) {
        return this.dataType().clazz().isInstance(value);
    }

    /**
     * Check type of all the values(may be some of list properties) valid
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
    public String schema() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema.propertyKey(\"").append(this.name).append("\")");
        sb.append(this.dataType.schema());
        sb.append(this.cardinality.schema());
        sb.append(this.propertiesSchema());
        sb.append(".ifNotExist()");
        sb.append(".create();");
        return sb.toString();
    }

    public static class Builder implements PropertyKeyBuilder {

        private PropertyKey propertyKey;
        private SchemaTransaction transaction;

        public Builder(String name, SchemaTransaction transaction) {
            this(new PropertyKey(name), transaction);
        }

        public Builder(PropertyKey propertyKey, SchemaTransaction transaction) {
            E.checkNotNull(propertyKey, "propertyKey");
            E.checkNotNull(transaction, "transaction");
            this.propertyKey = propertyKey;
            this.transaction = transaction;
        }

        @Override
        public PropertyKey create() {
            String name = this.propertyKey.name();
            StringUtil.checkName(name);

            PropertyKey propertyKey = this.transaction.getPropertyKey(name);
            if (propertyKey != null) {
                if (this.propertyKey.checkExist) {
                    throw new ExistedException("property key", name);
                }
                return propertyKey;
            }

            this.transaction.addPropertyKey(this.propertyKey);
            return this.propertyKey;
        }

        @Override
        public PropertyKey append() {
            throw new HugeException(
                      "Not support append action on property key");
        }

        @Override
        public PropertyKey eliminate() {
            throw new HugeException(
                      "Not support eliminate action on property key");
        }

        @Override
        public void remove() {
            this.transaction.removePropertyKey(this.propertyKey.name);
        }

        @Override
        public Builder asText() {
            this.propertyKey.dataType(DataType.TEXT);
            return this;
        }

        @Override
        public Builder asInt() {
            this.propertyKey.dataType(DataType.INT);
            return this;
        }

        @Override
        public Builder asTimestamp() {
            this.propertyKey.dataType(DataType.TIMESTAMP);
            return this;
        }

        @Override
        public Builder asUuid() {
            this.propertyKey.dataType(DataType.UUID);
            return this;
        }

        @Override
        public Builder asBoolean() {
            this.propertyKey.dataType(DataType.BOOLEAN);
            return this;
        }

        @Override
        public Builder asByte() {
            this.propertyKey.dataType(DataType.BYTE);
            return this;
        }

        @Override
        public Builder asBlob() {
            this.propertyKey.dataType(DataType.BLOB);
            return this;
        }

        @Override
        public Builder asDouble() {
            this.propertyKey.dataType(DataType.DOUBLE);
            return this;
        }

        @Override
        public Builder asFloat() {
            this.propertyKey.dataType(DataType.FLOAT);
            return this;
        }

        @Override
        public Builder asLong() {
            this.propertyKey.dataType(DataType.LONG);
            return this;
        }

        @Override
        public Builder valueSingle() {
            this.propertyKey.cardinality(Cardinality.SINGLE);
            return this;
        }

        @Override
        public Builder valueList() {
            this.propertyKey.cardinality(Cardinality.LIST);
            return this;
        }

        @Override
        public Builder valueSet() {
            this.propertyKey.cardinality(Cardinality.SET);
            return this;
        }

        public Builder ifNotExist() {
            this.propertyKey.checkExist = false;
            return this;
        }
    }
}
