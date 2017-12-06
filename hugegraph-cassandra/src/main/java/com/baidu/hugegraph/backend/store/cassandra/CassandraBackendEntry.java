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

package com.baidu.hugegraph.backend.store.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.HugeKeys;

public class CassandraBackendEntry implements BackendEntry {

    public static class Row {

        private HugeType type;
        private Id id;
        private Id subId;
        private Map<HugeKeys, Object> columns;

        public Row(HugeType type) {
            this(type, null);
        }

        public Row(HugeType type, Id id) {
            this.type = type;
            this.id = id;
            this.subId = null;
            this.columns = new ConcurrentHashMap<>();
        }

        public HugeType type() {
            return this.type;
        }

        public Id id() {
            return this.id;
        }

        public Map<HugeKeys, Object> columns() {
            return this.columns;
        }

        @SuppressWarnings("unchecked")
        public <T> T column(HugeKeys key) {
            // The T must be primitive type, or list/set/map of primitive type
            return (T) this.columns.get(key);
        }

        public <T> void column(HugeKeys key, T value) {
            this.columns.put(key, value);
        }

        public <T> void column(HugeKeys key, T value, Cardinality c) {
            switch (c) {
                case SINGLE:
                    this.column(key, value);
                    break;
                case SET:
                    this.columns.putIfAbsent(key,  new LinkedHashSet<>());
                    this.<Set<T>>column(key).add(value);
                    break;
                case LIST:
                    this.columns.putIfAbsent(key,  new LinkedList<>());
                    this.<List<T>>column(key).add(value);
                    break;
                default:
                    throw new AssertionError("Unsupported cardinality: " + c);
            }
        }

        public <T> void column(HugeKeys key, Long name, T value) {
            this.columns.putIfAbsent(key, new ConcurrentHashMap<>());
            this.<Map<Long, T>>column(key).put(name, value);
        }

        @Override
        public String toString() {
            return String.format("Row{type=%s, id=%s, columns=%s}",
                                 this.type, this.id, this.columns);
        }
    }

    private final Row row;
    private final List<Row> subRows;

    // NOTE: selfChanged is false when the row has not changed but subRows has.
    private boolean selfChanged = true;

    public CassandraBackendEntry(Id id) {
        this(null, id);
    }

    public CassandraBackendEntry(HugeType type) {
        this(type, null);
    }

    public CassandraBackendEntry(HugeType type, Id id) {
        this(new Row(type, id));
    }

    public CassandraBackendEntry(Row row) {
        this.row = row;
        this.subRows = new ArrayList<>();
        this.selfChanged = true;
    }

    @Override
    public HugeType type() {
        return this.row.type;
    }

    public void type(HugeType type) {
        this.row.type = type;
    }

    @Override
    public Id id() {
        return this.row.id;
    }

    public void id(Id id) {
        this.row.id = id;
    }

    @Override
    public Id subId() {
        return this.row.subId;
    }

    public void subId(Id subId) {
        this.row.subId = subId;
    }

    public void selfChanged(boolean changed) {
        this.selfChanged = changed;
    }

    public boolean selfChanged() {
        return this.selfChanged;
    }

    public Row row() {
        return this.row;
    }

    public Map<HugeKeys, Object> columnsMap() {
        return this.row.columns();
    }

    public <T> void column(HugeKeys key, T value) {
        this.row.column(key, value);
    }

    public <T> void column(HugeKeys key, Long name, T value) {
        this.row.column(key, name, value);
    }

    public <T> void column(HugeKeys key, T value, Cardinality c) {
        this.row.column(key, value, c);
    }

    public <T> T column(HugeKeys key) {
        return this.row.column(key);
    }

    public void subRow(Row row) {
        this.subRows.add(row);
    }

    public List<Row> subRows() {
        return this.subRows;
    }

    @Override
    public String toString() {
        return String.format("CassandraBackendEntry{%s, sub-rows: %s}",
                             this.row.toString(),
                             this.subRows.toString());
    }

    @Override
    public Collection<BackendColumn> columns() {
        throw new RuntimeException("Not supported by Cassandra");
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        throw new RuntimeException("Not supported by Cassandra");
    }

    @Override
    public void columns(BackendColumn... bytesColumns) {
        throw new RuntimeException("Not supported by Cassandra");
    }

    @Override
    public void merge(BackendEntry other) {
        throw new RuntimeException("Not supported by Cassandra");
    }

    @Override
    public void clear() {
        throw new RuntimeException("Not supported by Cassandra");
    }
}
