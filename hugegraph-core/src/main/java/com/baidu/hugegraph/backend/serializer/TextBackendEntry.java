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
package com.baidu.hugegraph.backend.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.util.StringEncoding;

public class TextBackendEntry implements BackendEntry {

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

    public void column(String column, String value) {
        this.columns.put(column, value);
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

    public void merge(TextBackendEntry other) {
        this.columns.putAll(other.columns);
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.id, this.columns.toString());
    }

    @Override
    public Collection<BackendColumn> columns() {
        List<BackendColumn> list = new ArrayList<BackendColumn>(this.columns.size());
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
}
