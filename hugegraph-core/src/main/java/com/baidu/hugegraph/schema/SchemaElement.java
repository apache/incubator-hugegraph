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
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.Typifiable;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.E;

public abstract class SchemaElement implements Namifiable, Typifiable,
                                               Cloneable {

    public static final int MAX_PRIMITIVE_SYS_ID = 32;
    public static final int NEXT_PRIMITIVE_SYS_ID = 7;

    protected final HugeGraph graph;

    private final Id id;
    private final String name;
    private final Userdata userdata;
    private SchemaStatus status;

    public SchemaElement(final HugeGraph graph, Id id, String name) {
        E.checkArgumentNotNull(id, "SchemaElement id can't be null");
        E.checkArgumentNotNull(name, "SchemaElement name can't be null");
        this.graph = graph;
        this.id = id;
        this.name = name;
        this.userdata = new Userdata();
        this.status = SchemaStatus.CREATED;
    }

    public HugeGraph graph() {
        return this.graph;
    }

    public Id id() {
        return this.id;
    }

    public long longId() {
        return this.id.asLong();
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

    public void userdata(Userdata userdata) {
        this.userdata.putAll(userdata);
    }

    public void removeUserdata(String key) {
        E.checkArgumentNotNull(key, "The userdata key can't be null");
        this.userdata.remove(key);
    }

    public void removeUserdata(Userdata userdata) {
        for (String key : userdata.keySet()) {
            this.userdata.remove(key);
        }
    }

    public SchemaStatus status() {
        return this.status;
    }

    public void status(SchemaStatus status) {
        this.status = status;
    }

    public boolean system() {
        return this.longId() < 0L;
    }

    public boolean primitive() {
        long id = this.longId();
        return -MAX_PRIMITIVE_SYS_ID <= id && id < 0L;
    }

    public boolean hidden() {
        return Graph.Hidden.isHidden(this.name());
    }

    public SchemaElement copy() {
        try {
            return (SchemaElement) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new HugeException("Failed to clone schema", e);
        }
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

    public static int schemaId(Id id) {
        long l = id.asLong();
        // Currently we limit the schema id to within 4 bytes
        E.checkArgument(Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE,
                        "Schema id is out of bound: %s", l);
        return (int) l;
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
