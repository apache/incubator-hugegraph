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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.Propfiable;
import com.baidu.hugegraph.type.Typifiable;
import com.baidu.hugegraph.util.E;

public abstract class SchemaElement
                implements Namifiable, Typifiable, Propfiable {

    protected Id id;
    protected String name;
    protected Set<Id> properties;

    public SchemaElement(Id id, String name) {
        E.checkNotNull(id, "id");
        E.checkNotNull(name, "name");
        this.id = id;
        this.name = name;
        this.properties = new HashSet<>();
    }

    public Id id() {
        return this.id;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String toString() {
        return String.format("%s(id=%s)", this.name, this.id);
    }

    @Override
    public Set<Id> properties() {
        return Collections.unmodifiableSet(this.properties);
    }

    public void properties(Set<Id> properties) {
        this.properties.addAll(properties);
    }

    public boolean primitive() {
        return false;
    }

    public boolean hidden() {
        return Graph.Hidden.isHidden(this.name());
    }

    public static Id schemaId(String id) {
        return IdGenerator.of(Long.valueOf(id));
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

    public static void checkName(String name, HugeConfig config) {
        String illegalReg = config.get(CoreOptions.SCHEMA_ILLEGAL_NAME_REGEX);

        E.checkNotNull(name, "name");
        E.checkArgument(!name.isEmpty(), "The name can't be empty.");
        E.checkArgument(name.length() < 256,
                        "The length of name must less than 256 bytes.");
        E.checkArgument(!name.matches(illegalReg),
                        String.format("Illegal schema name '%s'", name));

        final char[] filters = {'#', '>', ':', '!'};
        for (char c : filters) {
            E.checkArgument(name.indexOf(c) == -1,
                            "The name can't contain character '%s'.", c);
        }
    }
}
