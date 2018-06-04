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

package com.baidu.hugegraph.backend.store.mysql;

import java.util.Collection;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;

public class MysqlBackendEntry extends TableBackendEntry {

    public MysqlBackendEntry(Id id) {
        super(id);
    }

    public MysqlBackendEntry(HugeType type) {
        this(type, null);
    }

    public MysqlBackendEntry(HugeType type, Id id) {
        this(new Row(type, id));
    }

    public MysqlBackendEntry(TableBackendEntry.Row row) {
        super(row);
    }

    @Override
    public String toString() {
        return String.format("MysqlBackendEntry{%s, sub-rows: %s}",
                             this.row().toString(),
                             this.subRows().toString());
    }

    @Override
    public int columnsSize() {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public Collection<BackendColumn> columns() {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public void columns(BackendColumn... bytesColumns) {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public void merge(BackendEntry other) {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public void clear() {
        throw new RuntimeException("Not supported by MySQL");
    }
}
