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

import java.util.HashSet;
import java.util.Set;

import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.Propfiable;
import com.baidu.hugegraph.type.Typifiable;

public abstract class SchemaElement
                implements Namifiable, Typifiable, Propfiable {

    protected String name;
    protected boolean checkExist;
    protected Set<String> properties;

    public SchemaElement(String name) {
        this.name = name;
        this.checkExist = true;
        this.properties = new HashSet<>();
    }

    @Override
    public String name() {
        return this.name;
    }

    public boolean checkExist() {
        return this.checkExist;
    }

    public void checkExist(boolean checkExists) {
        this.checkExist = checkExists;
    }

    @Override
    public Set<String> properties() {
        return this.properties;
    }

    protected String propertiesSchema() {
        StringBuilder sb = new StringBuilder();
        for (String propertyName : this.properties) {
            sb.append("\"").append(propertyName).append("\",");
        }
        int endIdx = sb.lastIndexOf(",") > 0 ? sb.length() - 1 : sb.length();
        return String.format(".properties(%s)", sb.substring(0, endIdx));
    }

    @Override
    public String toString() {
        return schema();
    }

    public static boolean isSchema(HugeType type) {
        if (type == HugeType.VERTEX_LABEL ||
            type == HugeType.EDGE_LABEL ||
            type == HugeType.PROPERTY_KEY ||
            type == HugeType.INDEX_LABEL) {
            return true;
        }
        return false;
    }

    public abstract String schema();
}
