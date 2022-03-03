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

package com.baidu.hugegraph.space;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

public class SchemaTemplate {

    public static SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private String name;
    private String schema;

    protected Date create;
    protected String creator;

    public SchemaTemplate(String name, String schema) {
        E.checkArgument(name != null && !name.isEmpty(),
                        "The name of schema template can't be null or empty");
        E.checkArgument(schema != null && !schema.isEmpty(),
                        "The schema template can't be null or empty");
        this.name = name;
        this.schema = schema;
    }

    public SchemaTemplate(String name, String schema, Date create, String creator) {
        E.checkArgument(name != null && !name.isEmpty(),
                        "The name of schema template can't be null or empty");
        E.checkArgument(schema != null && !schema.isEmpty(),
                        "The schema template can't be null or empty");
        this.name = name;
        this.schema = schema;
        this.create = create;

        this.creator = creator;
    }

    public String name() {
        return this.name;
    }

    public String schema() {
        return this.schema;
    }

    public void schema(String schema) {
        this.schema = schema;
    }

    public Date create() {
        return this.create;
    }

    public Date update() {
        return this.create;
    }

    public void create(Date create) {
        this.create = create;
    }

    public String creator() {
        return this.creator;
    }

    public void creator(String creator) {
        this.creator = creator;
    }

    public Map<String, String> asMap() {
        return ImmutableMap.of("name", this.name,
                               "schema", this.schema,
                               "create", FORMATTER.format(this.create),
                               "update", FORMATTER.format(this.create),
                               "creator", this.creator);
    }

    public static SchemaTemplate fromMap(Map<String , String> map) {
        try {
            return new SchemaTemplate(map.get("name"),
                                      map.get("schema"),
                                      FORMATTER.parse(map.get("create")),
                                      map.get("creator"));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }
}
