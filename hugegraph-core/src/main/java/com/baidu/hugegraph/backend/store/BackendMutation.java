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
package com.baidu.hugegraph.backend.store;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.util.E;

public class BackendMutation {

    private final Map<Id, List<MutateItem>> updates;

    public BackendMutation() {
        this.updates = new LinkedHashMap<>();
    }

    /**
     * Add an action with data entry to updates collections
     */
    public void add(BackendEntry entry, MutateAction action) {
        Id id = entry.id();
        List<MutateItem> items = this.updates.get(id);
        // If there is no entity of this id, add it
        if (items == null) {
            items = new LinkedList<>();
            items.add(MutateItem.of(entry, action));
            this.updates.put(id, items);
            return;
        }

        /*
         * TODO: Should do some optimize when seperate edges from vertex
         * The Optimized scenes include but are not limited to：
         * 1.If you want to delete an entry, the other mutations previously
         *  can be ignored.
         * 2.As similar to the item No. one, If you want to insert an entry,
         *  the other mutations previously also can be ignored.
         * 3.To be added.
         */
        items.add(MutateItem.of(entry, action));
    }

    /**
     * Reset all items in mutations of this id.
     */
    public void reset(Id id) {
        this.updates.replace(id, new LinkedList<>());
    }

    public Map<Id, List<MutateItem>> mutation() {
        return Collections.unmodifiableMap(this.updates);
    }

    /**
     * Whether this mutation is empty
     * @return boolean
     */
    public boolean isEmpty() {
        return this.updates.isEmpty();
    }

    /**
     * Merges another mutation into this mutation. Ensures that all additions
     * and deletions are added to this mutation. Does not remove duplicates
     * if such exist - this needs to be ensured by the caller.
     */
    public void merge(BackendMutation mutation) {
        E.checkNotNull(mutation, "mutation");
        for (List<MutateItem> items : mutation.mutation().values()) {
            for (MutateItem item : items) {
                this.add(item.entry(), item.action());
            }
        }
    }

    @Override
    public String toString() {
        return String.format("BackendMutation{mutations=%s}", this.updates);
    }
}
