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

package com.baidu.hugegraph.schema;

import java.util.Arrays;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.util.StringUtil;


public class HugePropertyKey extends PropertyKey {

    private DataType dataType;
    private Cardinality cardinality;

    public HugePropertyKey(String name) {
        super(name);
        this.dataType = DataType.TEXT;
        this.cardinality = Cardinality.SINGLE;
    }

    @Override
    public HugePropertyKey indexNames(String... names) {
        this.indexNames.addAll(Arrays.asList(names));
        return this;
    }

    @Override
    public DataType dataType() {
        return this.dataType;
    }

    public void dataType(DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public Cardinality cardinality() {
        return this.cardinality;
    }

    public void cardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    public void checkExist(boolean checkExists) {
        this.checkExist = checkExists;
    }

    @Override
    public PropertyKey asText() {
        this.dataType(DataType.TEXT);
        return this;
    }

    @Override
    public PropertyKey asInt() {
        this.dataType(DataType.INT);
        return this;
    }

    @Override
    public PropertyKey asTimestamp() {
        this.dataType(DataType.TIMESTAMP);
        return this;
    }

    @Override
    public PropertyKey asUuid() {
        this.dataType(DataType.UUID);
        return this;
    }

    @Override
    public PropertyKey asBoolean() {
        this.dataType(DataType.BOOLEAN);
        return this;
    }

    @Override
    public PropertyKey asByte() {
        this.dataType(DataType.BYTE);
        return this;
    }

    @Override
    public PropertyKey asBlob() {
        this.dataType(DataType.BLOB);
        return this;
    }

    @Override
    public PropertyKey asDouble() {
        this.dataType(DataType.DOUBLE);
        return this;
    }

    @Override
    public PropertyKey asFloat() {
        this.dataType(DataType.FLOAT);
        return this;
    }

    @Override
    public PropertyKey asLong() {
        this.dataType(DataType.LONG);
        return this;
    }

    @Override
    public PropertyKey valueSingle() {
        this.cardinality(Cardinality.SINGLE);
        return this;
    }

    @Override
    public PropertyKey valueList() {
        this.cardinality(Cardinality.LIST);
        return this;
    }

    @Override
    public PropertyKey valueSet() {
        this.cardinality(Cardinality.SET);
        return this;
    }

    @Override
    public String schema() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema.makePropertyKey(\"").append(this.name).append("\")");
        sb.append(this.dataType.schema());
        sb.append(this.cardinality.schema());
        sb.append(this.propertiesSchema());
        sb.append(".ifNotExist()");
        sb.append(".create();");
        return sb.toString();
    }

    @Override
    public PropertyKey create() {

        StringUtil.checkName(this.name);
        // Try to read
        PropertyKey propertyKey = this.transaction().getPropertyKey(this.name);
        // if propertyKey exist and checkExist
        if (propertyKey != null) {
            if (this.checkExist) {
                throw new ExistedException("property key", this.name);
            } else {
                return propertyKey;
            }
        }

        this.transaction().addPropertyKey(this);
        return this;
    }

    @Override
    public PropertyKey append() {
        throw new HugeException("Not support append action on property key");
    }

    @Override
    public PropertyKey eliminate() {
        throw new HugeException("Not support eliminate action on property key");
    }

    @Override
    public void remove() {
        this.transaction().removePropertyKey(this.name);
    }

    @Override
    public SchemaElement copy() throws CloneNotSupportedException {
        throw new CloneNotSupportedException(
                  "Not support copy operation for property key");
    }
}
