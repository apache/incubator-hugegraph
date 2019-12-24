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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.IdPrefixQuery;
import com.baidu.hugegraph.backend.query.IdRangeQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class InMemoryDBTable extends BackendTable<BackendSession,
                                                  TextBackendEntry> {

    protected final Map<Id, BackendEntry> store;

    public InMemoryDBTable(HugeType type) {
        super(type.name());
        this.store = new ConcurrentHashMap<>();
    }

    public InMemoryDBTable(HugeType type, Map<Id, BackendEntry> store) {
        super(type.name());
        this.store = store;
    }

    protected Map<Id, BackendEntry> store() {
        return this.store;
    }

    @Override
    public void init(BackendSession session) {
        // pass
    }

    @Override
    public void clear(BackendSession session) {
        this.store.clear();
    }

    @Override
    public void insert(BackendSession session, TextBackendEntry entry) {
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
    public void delete(BackendSession session, TextBackendEntry entry) {
        // Remove by id (TODO: support remove by id + condition)
        this.store.remove(entry.id());
    }

    @Override
    public void append(BackendSession session, TextBackendEntry entry) {
        BackendEntry parent = this.store.get(entry.id());
        if (parent == null) {
            this.store.put(entry.id(), entry);
        } else {
            // TODO: Compatible with BackendEntry
            ((TextBackendEntry) parent).append(entry);
        }
    }

    @Override
    public void eliminate(BackendSession session, TextBackendEntry entry) {
        BackendEntry parent = this.store.get(entry.id());
        // TODO: Compatible with BackendEntry
        if (parent != null) {
            ((TextBackendEntry) parent).eliminate(entry);
        }
    }

    @Override
    public Iterator<BackendEntry> query(BackendSession session, Query query) {
        if (query.paging()) {
            throw new NotSupportException("paging by InMemoryDBStore");
        }

        Map<Id, BackendEntry> rs = this.store;

        if (query instanceof IdPrefixQuery) {
            IdPrefixQuery pq = (IdPrefixQuery) query;
            rs = this.queryByIdPrefix(pq.start(), pq.inclusiveStart(),
                                      pq.prefix(), rs);
        }

        if (query instanceof IdRangeQuery) {
            IdRangeQuery rq = (IdRangeQuery) query;
            rs = this.queryByIdRange(rq.start(), rq.inclusiveStart(),
                                     rq.end(), rq.inclusiveEnd(), rs);
        }

        // Query by id(s)
        if (!query.ids().isEmpty()) {
            rs = this.queryById(query.ids(), rs);
        }

        // Query by condition(s)
        if (!query.conditions().isEmpty()) {
            rs = this.queryByFilter(query.conditions(), rs);
        }

        Iterator<BackendEntry> iterator = rs.values().iterator();

        long offset = query.remainingOffset();
        if (offset >= rs.size()) {
            return QueryResults.emptyIterator();
        }
        iterator = this.skipOffset(iterator, offset);
        query.skipOffset(offset);

        if (!query.nolimit() && query.total() < rs.size()) {
            iterator = this.dropTails(iterator, query.limit());
        }
        return iterator;
    }

    protected Map<Id, BackendEntry> queryById(Set<Id> ids,
                                              Map<Id, BackendEntry> entries) {
        assert ids.size() > 0;
        Map<Id, BackendEntry> rs = InsertionOrderUtil.newMap();

        for (Id id : ids) {
            assert !id.number();
            if (entries.containsKey(id)) {
                rs.put(id, entries.get(id));
            }
        }
        return rs;
    }

    protected Map<Id, BackendEntry> queryByIdPrefix(Id start,
                                                    boolean inclusiveStart,
                                                    Id prefix,
                                                    Map<Id, BackendEntry> rs) {
        throw new BackendException("Unsupported prefix query: " + prefix);
    }

    protected Map<Id, BackendEntry> queryByIdRange(Id start,
                                                   boolean inclusiveStart,
                                                   Id end,
                                                   boolean inclusiveEnd,
                                                   Map<Id, BackendEntry> rs) {
        throw new BackendException("Unsupported range query: " + start);
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
        } else if (r.relation() == Condition.RelationType.CONTAINS_VALUE) {
            return entry.containsValue(r.serialValue().toString());
        } else if (r.relation() == Condition.RelationType.EQ) {
            return entry.contains(key, r.serialValue().toString());
        } else if (entry.contains(key)) {
            return r.test(entry.column(key));
        }
        return false;
    }
}
