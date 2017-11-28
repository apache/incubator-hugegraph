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

package com.baidu.hugegraph.backend.store.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;

public class InMemoryDBTable {

    private final String name;

    private final Map<Id, BackendEntry> store;

    public InMemoryDBTable(HugeType type) {
        this.name = type.name();
        this.store = new ConcurrentSkipListMap<>();
    }

    public String table() {
        return this.name;
    }

    public Map<Id, BackendEntry> store() {
        return this.store;
    }

    public void clear() {
        this.store.clear();
    }

    public void insert(BackendEntry entry) {
        if (!this.store.containsKey(entry.id())) {
            this.store.put(entry.id(), entry);
        } else {
            // Merge columns if the entry exists
            BackendEntry origin = this.store.get(entry.id());
            // TODO: Compatible with BackendEntry
            origin.merge(entry);
        }
    }

    public void delete(TextBackendEntry entry) {
        // Remove by id (TODO: support remove by id + condition)
        if (entry.type() == HugeType.EDGE) {
            BackendEntry parent = store.get(entry.id());
            if (parent != null) {
                ((TextBackendEntry) parent).eliminate(entry);
            }
        } else {
            store.remove(entry.id());
        }
    }

    public void append(TextBackendEntry entry) {
        BackendEntry parent = store.get(entry.id());
        if (parent == null) {
            store.put(entry.id(), entry);
        } else {
            // TODO: Compatible with BackendEntry
            ((TextBackendEntry) parent).append(entry);
        }
    }

    public void eliminate(TextBackendEntry entry) {
        BackendEntry parent = store.get(entry.id());
        // TODO: Compatible with BackendEntry
        if (parent != null) {
            ((TextBackendEntry) parent).eliminate(entry);
        }
    }
}