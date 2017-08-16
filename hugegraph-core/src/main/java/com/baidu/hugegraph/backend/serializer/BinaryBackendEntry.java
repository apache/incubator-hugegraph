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

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;

public class BinaryBackendEntry implements BackendEntry {

    private Id id;
    private Collection<BackendColumn> columns;

    public BinaryBackendEntry(Id id) {
        this.id = id;
        this.columns = new ArrayList<>();
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public void id(Id id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.id, this.columns.toString());
    }

    public BackendColumn column(byte[] name) {
        for (BackendColumn col : this.columns) {
            if (Arrays.equals(col.name, name)) {
                return col;
            }
        }
        return null;
    }

    public void column(BackendColumn column) {
        this.columns.add(column);
    }

    @Override
    public Collection<BackendColumn> columns() {
        return this.columns;
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        this.columns = bytesColumns;
    }
}
