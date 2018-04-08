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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class BackendMutation {

    private final MutationTable updates;

    public BackendMutation() {
        this.updates = new MutationTable();
    }

    /**
     * Add data entry with an action to collection `updates`
     */
    @Watched(prefix = "mutation")
    public void add(BackendEntry entry, Action action) {
        Id id = entry.id();
        assert id != null;
        if (this.updates.containsKey(entry.type(), id)) {
            this.optimizeUpdates(entry, action);
        } else {
            // If there is no entity with this id, add it
            this.updates.put(entry.type(), id, BackendAction.of(action, entry));
        }
    }

    /**
     * The optimized scenes include but are not limited to：
     * 1.If you want to delete an entry, the other mutations previously
     *   can be ignored.
     * 2.As similar to the No.1 item, If you want to insert an entry,
     *   the other mutations previously also can be ignored.
     * 3.If you append an entry and then eliminate it, the new action
     *   can override the old one.
     */
    @Watched(prefix = "mutation")
    private void optimizeUpdates(BackendEntry entry, Action action) {
        final Id id = entry.id();
        assert id != null;
        final List<BackendAction> items = this.updates.get(entry.type(), id);
        assert items != null;
        boolean ignoreCurrent = false;
        for (Iterator<BackendAction> itor = items.iterator(); itor.hasNext();) {
            BackendAction originItem = itor.next();
            Action originAction = originItem.action();
            switch (action) {
                case INSERT:
                    itor.remove();
                    break;
                case DELETE:
                    if (originAction == Action.INSERT) {
                        throw incompatibleActionException(action, originAction);
                    } else if (originAction == Action.DELETE) {
                        ignoreCurrent = true;
                    } else {
                        itor.remove();
                    }
                    break;
                case APPEND:
                case ELIMINATE:
                    if (originAction == Action.INSERT ||
                        originAction == Action.DELETE) {
                        throw incompatibleActionException(action, originAction);
                    } else {
                        Id subId = entry.subId();
                        Id originSubId = originItem.entry().subId();
                        assert subId != null;
                        if (subId == originSubId || subId.equals(originSubId)) {
                            itor.remove();
                        }
                    }
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Unknown mutate action: %s", action));
            }
        }
        if (!ignoreCurrent) {
            items.add(BackendAction.of(action, entry));
        }
    }

    private static HugeException incompatibleActionException(
                                 Action newAction,
                                 Action originAction) {
        return new HugeException("The action '%s' is incompatible with " +
                                 "action '%s'", newAction, originAction);
    }

    /**
     * Merges another mutation into this mutation. Ensures that all additions
     * and deletions are added to this mutation. Does not remove duplicates
     * if such exist - this needs to be ensured by the caller.
     */
    public void merge(BackendMutation mutation) {
        E.checkNotNull(mutation, "mutation");
        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            BackendAction item = it.next();
            this.add(item.entry(), item.action());
        }
    }

    /**
     * Get all types of updates
     * @return Iterator<MutateItem>
     */
    public Set<HugeType> types() {
        return this.updates.keys();
    }

    /**
     * Get all updates
     * @return Iterator<MutateItem>
     */
    public Iterator<BackendAction> mutation() {
        return this.updates.values();
    }

    /**
     * Get updates by type
     * @return Iterator<MutateItem>
     */
    public Iterator<BackendAction> mutation(HugeType type) {
        return this.updates.get(type);
    }

    /**
     * Get updates by type and id
     * @return Iterator<MutateItem>
     */
    public List<BackendAction> mutation(HugeType type, Id id) {
        return this.updates.get(type, id);
    }

    /**
     * Whether this mutation is empty
     * @return boolean
     */
    public boolean isEmpty() {
        return this.updates.size() == 0;
    }

    /**
     * Get size of id(s) to update
     * @return int
     */
    public int size() {
        return this.updates.size();
    }

    @Override
    public String toString() {
        return String.format("BackendMutation{mutations=%s}", this.updates);
    }

    private static class MutationTable {

        // Mapping type => id => mutations
        private final Map<HugeType, Map<Id, List<BackendAction>>> mutations;

        public MutationTable() {
            // NOTE: ensure insert order
            this.mutations = InsertionOrderUtil.newMap();
        }

        public void put(HugeType type, Id id, BackendAction mutation) {
            Map<Id, List<BackendAction>> table = this.mutations.get(type);
            if (table == null) {
                table = InsertionOrderUtil.newMap();
                this.mutations.put(type, table);
            }

            List<BackendAction> items = table.get(id);
            if (items == null) {
                items = new ArrayList<>();
                table.put(id, items);
            }

            items.add(mutation);
        }

        public boolean containsKey(HugeType type, Id id) {
            Map<Id, List<BackendAction>> table = this.mutations.get(type);
            return table != null && table.containsKey(id);
        }

        public List<BackendAction> get(HugeType type, Id id) {
            Map<Id, List<BackendAction>> table = this.mutations.get(type);
            if (table == null) {
                return null;
            }
            return table.get(id);
        }

        public Iterator<BackendAction> get(HugeType type) {
            ExtendableIterator<BackendAction> rs = new ExtendableIterator<>();
            Map<Id, List<BackendAction>> table = this.mutations.get(type);
            for (List<BackendAction> items : table.values()) {
                rs.extend(items.iterator());
            }
            return rs;
        }

        public Set<HugeType> keys() {
            return this.mutations.keySet();
        }

        public Iterator<BackendAction> values() {
            ExtendableIterator<BackendAction> rs = new ExtendableIterator<>();
            for (Map<Id, List<BackendAction>> table : this.mutations.values()) {
                for (List<BackendAction> items : table.values()) {
                    rs.extend(items.iterator());
                }
            }
            return rs;
        }

        public int size() {
            int size = 0;
            for (Map<Id, List<BackendAction>> m : this.mutations.values()) {
                size += m.size();
            }
            return size;
        }
    }
}
