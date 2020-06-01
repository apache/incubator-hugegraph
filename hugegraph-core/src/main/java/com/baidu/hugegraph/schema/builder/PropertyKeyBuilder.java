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

package com.baidu.hugegraph.schema.builder;

import java.util.Map;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.Userdata;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.type.define.AggregateType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;

public class PropertyKeyBuilder extends AbstractBuilder
                                implements PropertyKey.Builder {

    private Id id;
    private String name;
    private DataType dataType;
    private Cardinality cardinality;
    private AggregateType aggregateType;
    private boolean checkExist;
    private Userdata userdata;

    public PropertyKeyBuilder(SchemaTransaction transaction,
                              HugeGraph graph, String name) {
        super(transaction, graph);
        E.checkNotNull(name, "name");
        this.id = null;
        this.name = name;
        this.dataType = DataType.TEXT;
        this.cardinality = Cardinality.SINGLE;
        this.aggregateType = AggregateType.NONE;
        this.userdata = new Userdata();
        this.checkExist = true;
    }

    @Override
    public PropertyKey build() {
        Id id = this.validOrGenerateId(HugeType.PROPERTY_KEY,
                                       this.id, this.name);
        PropertyKey propertyKey = new PropertyKey(this.graph(), id, this.name);
        propertyKey.dataType(this.dataType);
        propertyKey.cardinality(this.cardinality);
        propertyKey.aggregateType(this.aggregateType);
        propertyKey.userdata(this.userdata);
        return propertyKey;
    }

    @Override
    public PropertyKey create() {
        HugeType type = HugeType.PROPERTY_KEY;
        this.checkSchemaName(this.name);

        return this.lockCheckAndCreateSchema(type, this.name, name -> {
            PropertyKey propertyKey = this.propertyKeyOrNull(name);
            if (propertyKey != null) {
                if (this.checkExist || !hasSameProperties(propertyKey)) {
                    throw new ExistedException(type, name);
                }
                return propertyKey;
            }
            this.checkSchemaIdIfRestoringMode(type, this.id);

            Userdata.check(this.userdata, Action.INSERT);
            this.checkAggregateType();

            propertyKey = this.build();
            assert propertyKey.name().equals(name);
            this.graph().addPropertyKey(propertyKey);
            return propertyKey;
        });
    }


    /**
     * Check whether this has same properties with propertyKey.
     * Only dataType, cardinality, aggregateType are checked.
     * The id, checkExist, userdata are not checked.
     * @param propertyKey to be compared with
     * @return true if this has same properties with propertyKey
     */
    private boolean hasSameProperties(PropertyKey propertyKey) {
        // dataType is enum
        if (this.dataType != propertyKey.dataType()) {
            return false;
        }

        // cardinality is enum
        if (this.cardinality != propertyKey.cardinality()) {
            return false;
        }

        // aggregateType is enum
        if (this.aggregateType != propertyKey.aggregateType()) {
            return false;
        }

        // all properties are same, return true.
        return true;
    }

    @Override
    public PropertyKey append() {
        PropertyKey propertyKey = this.propertyKeyOrNull(this.name);
        if (propertyKey == null) {
            throw new NotFoundException("Can't update property key '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        Userdata.check(this.userdata, Action.APPEND);

        propertyKey.userdata(this.userdata);
        this.graph().addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public PropertyKey eliminate() {
        PropertyKey propertyKey = this.propertyKeyOrNull(this.name);
        if (propertyKey == null) {
            throw new NotFoundException("Can't update property key '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        Userdata.check(this.userdata, Action.ELIMINATE);

        propertyKey.removeUserdata(this.userdata);
        this.graph().addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public Id remove() {
        PropertyKey propertyKey = this.propertyKeyOrNull(this.name);
        if (propertyKey == null) {
            return null;
        }
        this.graph().removePropertyKey(propertyKey.id());
        return null;
    }

    @Override
    public PropertyKeyBuilder id(long id) {
        E.checkArgument(id != 0L,
                        "Not allowed to assign 0 as property key id");
        this.id = IdGenerator.of(id);
        return this;
    }

    @Override
    public PropertyKeyBuilder asText() {
        this.dataType = DataType.TEXT;
        return this;
    }

    @Override
    public PropertyKeyBuilder asInt() {
        this.dataType = DataType.INT;
        return this;
    }

    @Override
    public PropertyKeyBuilder asDate() {
        this.dataType = DataType.DATE;
        return this;
    }

    @Override
    public PropertyKeyBuilder asUUID() {
        this.dataType = DataType.UUID;
        return this;
    }

    @Override
    public PropertyKeyBuilder asBoolean() {
        this.dataType = DataType.BOOLEAN;
        return this;
    }

    @Override
    public PropertyKeyBuilder asByte() {
        this.dataType = DataType.BYTE;
        return this;
    }

    @Override
    public PropertyKeyBuilder asBlob() {
        this.dataType = DataType.BLOB;
        return this;
    }

    @Override
    public PropertyKeyBuilder asDouble() {
        this.dataType = DataType.DOUBLE;
        return this;
    }

    @Override
    public PropertyKeyBuilder asFloat() {
        this.dataType = DataType.FLOAT;
        return this;
    }

    @Override
    public PropertyKeyBuilder asLong() {
        this.dataType = DataType.LONG;
        return this;
    }

    @Override
    public PropertyKeyBuilder valueSingle() {
        this.cardinality = Cardinality.SINGLE;
        return this;
    }

    @Override
    public PropertyKeyBuilder valueList() {
        this.cardinality = Cardinality.LIST;
        return this;
    }

    @Override
    public PropertyKeyBuilder valueSet() {
        this.cardinality = Cardinality.SET;
        return this;
    }

    @Override
    public PropertyKeyBuilder calcMax() {
        this.aggregateType = AggregateType.MAX;
        return this;
    }

    @Override
    public PropertyKeyBuilder calcMin() {
        this.aggregateType = AggregateType.MIN;
        return this;
    }

    @Override
    public PropertyKeyBuilder calcSum() {
        this.aggregateType = AggregateType.SUM;
        return this;
    }

    @Override
    public PropertyKeyBuilder calcOld() {
        this.aggregateType = AggregateType.OLD;
        return this;
    }

    @Override
    public PropertyKeyBuilder userdata(String key, Object value) {
        this.userdata.put(key, value);
        return this;
    }

    @Override
    public PropertyKeyBuilder cardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
        return this;
    }

    @Override
    public PropertyKeyBuilder dataType(DataType dataType) {
        this.dataType = dataType;
        return this;
    }

    @Override
    public PropertyKey.Builder aggregateType(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
        return this;
    }

    @Override
    public PropertyKeyBuilder userdata(Map<String, Object> userdata) {
        this.userdata.putAll(userdata);
        return this;
    }

    @Override
    public PropertyKeyBuilder ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public PropertyKeyBuilder checkExist(boolean checkExist) {
        this.checkExist = checkExist;
        return this;
    }

    private void checkStableVars() {
        if (this.dataType != DataType.TEXT) {
            throw new NotAllowException("Not allowed to update data type " +
                                        "for property key '%s'", this.name);
        }
        if (this.cardinality != Cardinality.SINGLE) {
            throw new NotAllowException("Not allowed to update cardinality " +
                                        "for property key '%s'", this.name);
        }
    }

    private void checkAggregateType() {
        if (this.aggregateType.isNone()) {
            return;
        }

        if (this.cardinality != Cardinality.SINGLE) {
            throw new NotAllowException("Not allowed to set aggregate type " +
                                        "'%s' for property key '%s' with " +
                                        "cardinality '%s'",
                                        this.aggregateType, this.name,
                                        this.cardinality);
        }

        if (this.aggregateType.isNumber() &&
            !this.dataType.isNumber() && !this.dataType.isDate()) {
            throw new NotAllowException(
                      "Not allowed to set aggregate type '%s' for " +
                      "property key '%s' with data type '%s'",
                      this.aggregateType, this.name, this.dataType);
        }
    }
}
