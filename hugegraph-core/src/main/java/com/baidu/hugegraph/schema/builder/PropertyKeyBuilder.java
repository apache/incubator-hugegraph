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

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;

public class PropertyKeyBuilder implements PropertyKey.Builder {

    private String name;
    private DataType dataType;
    private Cardinality cardinality;
    private Map<String, Object> userdata;
    private boolean checkExist;

    private SchemaTransaction transaction;

    public PropertyKeyBuilder(String name, SchemaTransaction transaction) {
        E.checkNotNull(name, "name");
        E.checkNotNull(transaction, "transaction");
        this.name = name;
        this.dataType = DataType.TEXT;
        this.cardinality = Cardinality.SINGLE;
        this.userdata = new HashMap<>();
        this.checkExist = true;
        this.transaction = transaction;
    }

    @Override
    public PropertyKey build() {
        HugeGraph graph = this.transaction.graph();
        Id id = this.transaction.getNextId(HugeType.PROPERTY_KEY);
        PropertyKey propertyKey = new PropertyKey(graph, id, this.name);
        propertyKey.dataType(this.dataType);
        propertyKey.cardinality(this.cardinality);
        for (Map.Entry<String, Object> entry : this.userdata.entrySet()) {
            propertyKey.userdata(entry.getKey(), entry.getValue());
        }
        return propertyKey;
    }

    @Override
    public PropertyKey create() {
        SchemaElement.checkName(this.name,
                                this.transaction.graph().configuration());
        PropertyKey propertyKey = this.transaction.getPropertyKey(this.name);
        if (propertyKey != null) {
            if (this.checkExist) {
                throw new ExistedException("property key", this.name);
            }
            return propertyKey;
        }
        this.checkUserData(Action.INSERT);

        propertyKey = this.build();
        this.transaction.addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public PropertyKey append() {
        PropertyKey propertyKey = this.transaction.getPropertyKey(this.name);
        if (propertyKey == null) {
            throw new NotFoundException("Can't update property key '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        this.checkUserData(Action.APPEND);

        for (Map.Entry<String, Object> entry : this.userdata.entrySet()) {
            propertyKey.userdata(entry.getKey(), entry.getValue());
        }
        this.transaction.addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public PropertyKey eliminate() {
        PropertyKey propertyKey = this.transaction.getPropertyKey(this.name);
        if (propertyKey == null) {
            throw new NotFoundException("Can't update property key '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        this.checkUserData(Action.ELIMINATE);

        for (String key : this.userdata.keySet()) {
            propertyKey.removeUserData(key);
        }
        this.transaction.addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public void remove() {
        PropertyKey propertyKey = this.transaction.getPropertyKey(this.name);
        if (propertyKey == null) {
            return;
        }
        this.transaction.removePropertyKey(propertyKey.id());
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
    public PropertyKeyBuilder asUuid() {
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

    private void checkUserData(Action action) {
        switch (action) {
            case INSERT:
            case APPEND:
                for (Map.Entry<String, Object> e : this.userdata.entrySet()) {
                    if (e.getValue() == null) {
                        throw new NotAllowException(
                                  "Not allowed pass null userdata value when " +
                                  "create or append property key");
                    }
                }
                break;
            case ELIMINATE:
            case DELETE:
                // pass
                break;
            default:
                throw new AssertionError(String.format(
                          "Unknown schema action '%s'", action));
        }
    }
}
