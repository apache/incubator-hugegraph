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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.util.E;

public class BackendMutation {

    private final Map<Id, List<MutateItem>> updates;

    public BackendMutation() {
        this.updates = new HashMap<>();
    }

    @Watched(prefix = "bm")
    /**
     * Add an action with data entry to updates collections
     */
    public void add(BackendEntry entry, MutateAction action) {
        Id id = entry.id();
        assert id != null;
        if (this.updates.containsKey(id)) {
            this.optimizeUpdates(entry, action);
        } else {
            // If there is no entity of this id, add it
            List<MutateItem> items = new LinkedList<>();
            items.add(MutateItem.of(entry, action));
            this.updates.put(id, items);
        }
    }

    @Watched(prefix = "bm")
    /**
     * The Optimized scenes include but are not limited to：
     * 1.If you want to delete an entry, the other mutations previously
     *   can be ignored.
     * 2.As similar to the No.1 item, If you want to insert an entry,
     *   the other mutations previously also can be ignored.
     * 3.If you append an entry and then eliminate it, the new action
     *   can override the old one.
     */
    private void optimizeUpdates(BackendEntry entry, MutateAction action) {
        final Id id = entry.id();
        assert id != null;
        final List<MutateItem> items = this.updates.get(id);
        boolean ignoreCurrent = false;
        for (Iterator<MutateItem> itor = items.iterator(); itor.hasNext();) {
            MutateItem originItem = itor.next();
            MutateAction originAction = originItem.action();
            switch (action) {
                case INSERT:
                    itor.remove();
                    break;
                case DELETE:
                    if (originAction == MutateAction.INSERT) {
                        throw incompatibleActionException(action, originAction);
                    } else if (originAction == MutateAction.DELETE) {
                        ignoreCurrent = true;
                    } else {
                        itor.remove();
                    }
                    break;
                case APPEND:
                case ELIMINATE:
                    if (originAction == MutateAction.INSERT ||
                        originAction == MutateAction.DELETE) {
                        throw incompatibleActionException(action, originAction);
                    } else {
                        assert entry.subId() != null;
                        if (entry.subId() == originItem.entry().subId() ||
                            entry.subId().equals(originItem.entry().subId())) {
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
            items.add(MutateItem.of(entry, action));
        }
    }

    private static HugeException incompatibleActionException(
                                 MutateAction newAction,
                                 MutateAction originAction) {
        return new HugeException("The action '%s' is incompatible with " +
                                 "action '%s'", newAction, originAction);
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
