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

import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.Propfiable;
import com.baidu.hugegraph.type.Typifiable;
import com.baidu.hugegraph.util.E;

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
        return Collections.unmodifiableSet(this.properties);
    }

    public boolean hidden() {
        return Graph.Hidden.isHidden(this.name());
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
        return this.schema();
    }

    public abstract String schema();

    public static void checkName(String name, String illegalRegex) {
        E.checkNotNull(name, "name");
        E.checkArgument(!name.isEmpty(), "The name can't be empty.");
        E.checkArgument(name.length() < 256,
                        "The length of name must less than 256 bytes.");
        E.checkArgument(!name.matches(illegalRegex),
                        String.format("Illegal schema name '%s'", name));

        final char[] filters = {'#', '>', ':', '!'};
        for (char c : filters) {
            E.checkArgument(name.indexOf(c) == -1,
                            "The name can't contain character '%s'.", c);
        }
    }
}
