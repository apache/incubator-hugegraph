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
import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.Typifiable;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.E;

public abstract class SchemaElement implements Namifiable, Typifiable {

    public static final int MAX_SYS_ID = 32;
    public static final int MIN_SYS_ID = 7;

    protected final HugeGraph graph;

    private final Id id;
    private final String name;
    private final Map<String, Object> userdata;
    private SchemaStatus status;

    public SchemaElement(final HugeGraph graph, Id id, String name) {
        E.checkArgumentNotNull(id, "SchemaElement id can't be null");
        E.checkArgumentNotNull(name, "SchemaElement name can't be null");
        this.graph = graph;
        this.id = id;
        this.name = name;
        this.userdata = new HashMap<>();
        this.status = SchemaStatus.CREATED;
    }

    public HugeGraph graph() {
        return this.graph;
    }

    public Id id() {
        return this.id;
    }

    @Override
    public String name() {
        return this.name;
    }

    public Map<String, Object> userdata() {
        return Collections.unmodifiableMap(this.userdata);
    }

    public void userdata(String key, Object value) {
        E.checkArgumentNotNull(key, "userdata key");
        E.checkArgumentNotNull(value, "userdata value");
        this.userdata.put(key, value);
    }

    public Object removeUserdata(String key) {
        E.checkArgumentNotNull(key, "The userdata key can't be null");
        return this.userdata.remove(key);
    }

    public SchemaStatus status() {
        return this.status;
    }

    public void status(SchemaStatus status) {
        this.status = status;
    }

    public boolean primitive() {
        return false;
    }

    public boolean hidden() {
        return Graph.Hidden.isHidden(this.name());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SchemaElement)) {
            return false;
        }

        SchemaElement other = (SchemaElement) obj;
        return this.type() == other.type() && this.id.equals(other.id());
    }

    @Override
    public int hashCode() {
        return this.type().hashCode() ^  this.id.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s(id=%s)", this.name, this.id);
    }

    public static Id schemaId(String id) {
        return IdGenerator.of(Long.parseLong(id));
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
