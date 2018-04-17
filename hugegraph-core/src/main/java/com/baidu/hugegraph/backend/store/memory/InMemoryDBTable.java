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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendSessionPool.Session;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;

public class InMemoryDBTable extends BackendTable<Session, TextBackendEntry> {

    protected final Map<Id, BackendEntry> store;

    public InMemoryDBTable(HugeType type) {
        super(type.name());
        this.store = new ConcurrentSkipListMap<>();
    }

    public InMemoryDBTable(HugeType type, Map<Id, BackendEntry> store) {
        super(type.name());
        this.store = store;
    }

    protected Map<Id, BackendEntry> store() {
        return this.store;
    }

    @Override
    public void init(Session session) {
        // pass
    }

    @Override
    public void clear(Session session) {
        this.store.clear();
    }

    @Override
    public void insert(Session session, TextBackendEntry entry) {
        if (!this.store.containsKey(entry.id())) {
            this.store.put(entry.id(), entry);
        } else {
            // Merge columns if the entry exists
            BackendEntry origin = this.store.get(entry.id());
            // TODO: Compatible with BackendEntry
            origin.merge(entry);
        }
    }

    @Override
    public void delete(Session session, TextBackendEntry entry) {
        // Remove by id (TODO: support remove by id + condition)
        this.store.remove(entry.id());
    }

    @Override
    public void append(Session session, TextBackendEntry entry) {
        BackendEntry parent = this.store.get(entry.id());
        if (parent == null) {
            this.store.put(entry.id(), entry);
        } else {
            // TODO: Compatible with BackendEntry
            ((TextBackendEntry) parent).append(entry);
        }
    }

    @Override
    public void eliminate(Session session, TextBackendEntry entry) {
        BackendEntry parent = this.store.get(entry.id());
        // TODO: Compatible with BackendEntry
        if (parent != null) {
            ((TextBackendEntry) parent).eliminate(entry);
        }
    }

    @Override
    public Iterator<BackendEntry> query(Session session, Query query) {
        if (query.paging()) {
            throw new NotSupportException("paging by InMemoryDBStore");
        }

        Map<Id, BackendEntry> rs = this.store;

        // Query by id(s)
        if (!query.ids().isEmpty()) {
            if (query.resultType().isEdge()) {
                E.checkState(query.conditions().isEmpty(),
                             "Not support querying edge by %s", query);
                // Query edge(in a vertex) by id (or v-id + column-name prefix)
                // TODO: separate this method into table Edge
                rs = this.queryEdgeById(query.ids(), rs);
            } else {
                rs = this.queryById(query.ids(), rs);
            }
        }

        // Query by condition(s)
        if (!query.conditions().isEmpty()) {
            if (query.resultType().isEdge()) {
                // TODO: separate this method into table Edge
                rs = this.queryEdgeByFilter(query.conditions(), rs);
            } else {
                rs = this.queryByFilter(query.conditions(), rs);
            }
        }

        Iterator<BackendEntry> iterator = rs.values().iterator();

        if (query.offset() >= rs.size()) {
            return Collections.emptyIterator();
        }
        iterator = this.skipOffset(iterator, query.offset());

        if (query.limit() != Query.NO_LIMIT &&
            query.offset() + query.limit() < rs.size()) {
            iterator = this.dropTails(iterator, query.limit());
        }
        return iterator;
    }

    protected Map<Id, BackendEntry> queryById(Set<Id> ids,
                                              Map<Id, BackendEntry> entries) {
        assert ids.size() > 0;
        Map<Id, BackendEntry> rs = new HashMap<>();

        for (Id id : ids) {
            assert !id.number();
            if (entries.containsKey(id)) {
                rs.put(id, entries.get(id));
            }
        }
        return rs;
    }

    protected Map<Id, BackendEntry> queryEdgeById(
                                    Set<Id> ids,
                                    Map<Id, BackendEntry> entries) {
        assert ids.size() > 0;
        Map<Id, BackendEntry> rs = new HashMap<>();

        for (Id id : ids) {
            // TODO: improve id split
            String[] parts = EdgeId.split(id);
            Id entryId = IdGenerator.of(parts[0]);

            String column = null;
            if (parts.length > 1) {
                parts = Arrays.copyOfRange(parts, 1, parts.length);
                column = EdgeId.concat(parts);
            } else {
                // All edges
                assert parts.length == 1;
            }

            if (entries.containsKey(entryId)) {
                BackendEntry value = entries.get(entryId);
                // TODO: Compatible with BackendEntry
                TextBackendEntry entry = (TextBackendEntry) value;
                if (column == null) {
                    // All edges in the vertex
                    rs.put(entryId, entry);
                } else if (entry.containsPrefix(column)) {
                    // An edge in the vertex
                    BackendEntry edges = new TextBackendEntry(HugeType.VERTEX,
                                                              entryId);
                    edges.columns(entry.columnsWithPrefix(column));

                    BackendEntry result = rs.get(entryId);
                    if (result == null) {
                        rs.put(entryId, edges);
                    } else {
                        result.merge(edges);
                    }
                }
            }
        }

        return rs;
    }

    protected Map<Id, BackendEntry> queryByFilter(
                                    Set<Condition> conditions,
                                    Map<Id, BackendEntry> entries) {
        assert conditions.size() > 0;

        Map<Id, BackendEntry> rs = new HashMap<>();

        for (BackendEntry entry : entries.values()) {
            // Query by conditions
            boolean matched = true;
            for (Condition c : conditions) {
                if (!matchCondition(entry, c)) {
                    // TODO: deal with others Condition like: and, or...
                    matched = false;
                    break;
                }
            }
            if (matched) {
                rs.put(entry.id(), entry);
            }
        }
        return rs;
    }

    protected Map<Id, BackendEntry> queryEdgeByFilter(
                                    Set<Condition> conditions,
                                    Map<Id, BackendEntry> entries) {
        if (conditions.isEmpty()) {
            return entries;
        }

        // Only support querying edge by label
        E.checkState(conditions.size() == 1,
                     "Not support querying edge by %s", conditions);
        Condition cond = conditions.iterator().next();
        E.checkState(cond.isRelation() &&
                     ((Condition.Relation) cond).key().equals(HugeKeys.LABEL),
                     "Not support querying edge by %s", conditions);
        Condition.Relation relation = (Condition.Relation) cond;
        String label = (String) relation.serialValue();

        Map<Id, BackendEntry> rs = new HashMap<>();

        for (BackendEntry value : entries.values()) {
            // TODO: Compatible with BackendEntry
            TextBackendEntry entry = (TextBackendEntry) value;
            String out = EdgeId.concat(HugeType.EDGE_OUT.string(), label);
            String in = EdgeId.concat(HugeType.EDGE_IN.string(), label);
            if (entry.containsPrefix(out)) {
                BackendEntry edges = new TextBackendEntry(HugeType.VERTEX,
                                                          entry.id());
                edges.columns(entry.columnsWithPrefix(out));
                rs.put(edges.id(), edges);
            }
            if (entry.containsPrefix(in)) {
                BackendEntry edges = new TextBackendEntry(HugeType.VERTEX,
                                                          entry.id());
                edges.columns(entry.columnsWithPrefix(in));
                BackendEntry result = rs.get(edges.id());
                if (result == null) {
                    rs.put(edges.id(), edges);
                } else {
                    result.merge(edges);
                }
            }
        }

        return rs;
    }

    protected Iterator<BackendEntry> skipOffset(Iterator<BackendEntry> iterator,
                                                long offset) {
        // Skip offset (TODO: maybe we can improve when adding items to rs)
        for (long i = 0; i < offset && iterator.hasNext(); i++) {
            iterator.next();
        }
        return iterator;
    }

    protected Iterator<BackendEntry> dropTails(Iterator<BackendEntry> iterator,
                                               long limit) {
        E.checkArgument(limit <= Integer.MAX_VALUE,
                        "Limit must be <= 0x7fffffff, but got '%s'", limit);
        List<BackendEntry> entries = new ArrayList<>((int) limit);
        for (long i = 0L; i < limit && iterator.hasNext(); i++) {
            entries.add(iterator.next());
        }
        return entries.iterator();
    }

    private static boolean matchCondition(BackendEntry item, Condition c) {
        // TODO: Compatible with BackendEntry
        TextBackendEntry entry = (TextBackendEntry) item;

        // Not supported by memory
        if (!(c instanceof Condition.Relation)) {
            throw new BackendException("Unsupported condition: " + c);
        }

        Condition.Relation r = (Condition.Relation) c;
        String key = r.serialKey().toString();

        // TODO: deal with others Relation like: <, >=, ...
        if (r.relation() == Condition.RelationType.CONTAINS_KEY) {
            return entry.contains(r.serialValue().toString());
        } else if (r.relation() == Condition.RelationType.CONTAINS) {
            return entry.containsValue(r.serialValue().toString());
        } else if (r.relation() == Condition.RelationType.EQ) {
            return entry.contains(key, r.serialValue().toString());
        } else if (entry.contains(key)) {
            return r.test(entry.column(key));
        }
        return false;
    }
}
