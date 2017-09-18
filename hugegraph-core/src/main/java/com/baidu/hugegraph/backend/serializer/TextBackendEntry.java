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

package com.baidu.hugegraph.backend.serializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.StringEncoding;

public class TextBackendEntry implements BackendEntry {

    public static final String COLUME_SPLITOR = "\u0001";
    public static final String VALUE_SPLITOR = "\u0002";

    private Id id;
    private Map<String, String> columns;

    public TextBackendEntry(Id id) {
        this.id = id;
        this.columns = new ConcurrentHashMap<>();
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public void id(Id id) {
        this.id = id;
    }

    public Set<String> columnNames() {
        return this.columns.keySet();
    }

    public void column(HugeKeys column, String value) {
        this.columns.put(column.string(), value);
    }

    public void column(String column, String value) {
        this.columns.put(column, value);
    }

    public String column(HugeKeys colume) {
        return this.columns.get(colume.string());
    }

    public String column(String colume) {
        return this.columns.get(colume);
    }

    public boolean contains(String colume) {
        return this.columns.containsKey(colume);
    }

    public boolean contains(String colume, String value) {
        return this.columns.containsKey(colume) &&
               this.columns.get(colume).equals(value);
    }

    public boolean containsPrefix(String column) {
        for (String c : this.columns.keySet()) {
            if (c.startsWith(column)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsValue(String value) {
        return this.columns.values().contains(value);
    }

    public Collection<BackendColumn> columnsWithPrefix(String column) {
        List<BackendColumn> list = new ArrayList<>();
        for (String c : this.columns.keySet()) {
            if (c.startsWith(column)) {
                String v = this.columns.get(c);
                BackendColumn bytesColumn = new BackendColumn();
                bytesColumn.name = StringEncoding.encodeString(c);
                bytesColumn.value = StringEncoding.encodeString(v);
                list.add(bytesColumn);
            }
        }
        return list;
    }

    public void append(TextBackendEntry entry) {
        for (Entry<String, String> col : entry.columns.entrySet()) {
            String newValue = col.getValue();
            String oldValue = this.column(col.getKey());
            if (newValue.equals(oldValue)) {
                continue;
            }
            // TODO: ensure the old value is a list and json format
            List<String> values = new ArrayList<>();
            values.addAll(Arrays.asList(JsonUtil.fromJson(oldValue,
                                                          String[].class)));
            values.addAll(Arrays.asList(JsonUtil.fromJson(newValue,
                                                          String[].class)));
            // Update the old value
            this.column(col.getKey(), JsonUtil.toJson(values));
        }
    }

    public void eliminate(TextBackendEntry entry) {
        for (Entry<String, String> col : entry.columns.entrySet()) {
            String newValue = col.getValue();
            String oldValue = this.column(col.getKey());
            if (newValue.equals(oldValue)) {
                // TODO: use more general method
                if (col.getKey().startsWith(HugeType.PROPERTY.name())) {
                    this.columns.remove(col.getKey());
                }
                continue;
            }
            // TODO: ensure the old value is a list and json format
            List<String> values = new ArrayList<>();
            values.addAll(Arrays.asList(JsonUtil.fromJson(oldValue,
                                                          String[].class)));
            values.removeAll(Arrays.asList(JsonUtil.fromJson(newValue,
                                                             String[].class)));
            // Update the old value
            this.column(col.getKey(), JsonUtil.toJson(values));
        }
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.id, this.columns.toString());
    }

    @Override
    public Collection<BackendColumn> columns() {
        List<BackendColumn> list = new ArrayList<>(this.columns.size());
        for (Entry<String, String> column : this.columns.entrySet()) {
            BackendColumn bytesColumn = new BackendColumn();
            bytesColumn.name = StringEncoding.encodeString(column.getKey());
            bytesColumn.value = StringEncoding.encodeString(column.getValue());
            list.add(bytesColumn);
        }
        return list;
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        this.columns.clear();

        for (BackendColumn column : bytesColumns) {
            this.columns.put(StringEncoding.decodeString(column.name),
                             StringEncoding.decodeString(column.value));
        }
    }

    @Override
    public void merge(BackendEntry other) {
        TextBackendEntry text = (TextBackendEntry) other;
        this.columns.putAll(text.columns);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TextBackendEntry)) {
            return false;
        }
        TextBackendEntry other = (TextBackendEntry) obj;
        if (this.id() != other.id() && !this.id().equals(other.id())) {
            return false;
        }
        if (this.columns().size() != other.columns().size()) {
            return false;
        }
        for (String key : this.columns.keySet()) {
            String value = this.columns.get(key);
            String otherValue = other.columns.get(key);
            if (otherValue == null) {
                return false;
            }
            if (value.equals(otherValue)) {
                return false;
            }
        }
        return true;
    }
}
