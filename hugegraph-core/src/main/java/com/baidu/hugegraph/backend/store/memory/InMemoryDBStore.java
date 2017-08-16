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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.MutateItem;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;

/**
 * NOTE:
 * InMemoryDBStore support:
 * 1.query by id (include query edges by id)
 * 2.query by condition (include query edges by condition)
 * 3.remove by id
 * 4.range query
 * 5.append/subtract index data(element-id) and vertex-property
 * InMemoryDBStore not support currently:
 * 1.remove by id + condition
 * 2.append/subtract edge-property
 * 3.query edge by edge-label
 */
public class InMemoryDBStore implements BackendStore {

    private static final Logger logger =
            LoggerFactory.getLogger(InMemoryDBStore.class);

    private final BackendStoreProvider provider;
    private final String name;
    private final ConcurrentNavigableMap<Id, BackendEntry> store;

    public InMemoryDBStore(final BackendStoreProvider provider,
                           final String name) {
        this.provider = provider;
        this.name = name;
        this.store = new ConcurrentSkipListMap<Id, BackendEntry>();
    }

    @Override
    public Object metadata(HugeType type, String meta, Object[] args) {
        throw new UnsupportedOperationException("InMemoryDBStore.metadata()");
    }

    @Override
    public Iterable<BackendEntry> query(final Query query) {
        Map<Id, BackendEntry> rs = null;

        // Filter by type (TODO: maybe we should let all id prefix with type)
        if (SchemaElement.isSchema(query.resultType())) {
            rs = queryPrefixWith(query.resultType().code());
        } else {
            rs = queryAll();
        }

        // Query by id(s)
        if (!query.ids().isEmpty()) {
            if (query.resultType() == HugeType.EDGE) {
                // Query edge(in a vertex) by id (or v-id + column-name prefix)
                // TODO: separate this method into a class
                rs = queryEdgeById(query.ids(), rs);
                E.checkState(query.conditions().isEmpty(),
                             "Not support querying edge by %s",
                             query.conditions());
            } else {
                rs = queryById(query.ids(), rs);
            }
        }

        // Query by condition(s)
        if (!query.conditions().isEmpty()) {
            rs = queryByFilter(query.conditions(), rs);
        }

        logger.info("[store {}] return {} for query: {}",
                    this.name, rs.values(), query);
        return rs.values();
    }

    protected Map<Id, BackendEntry> queryAll() {
        return Collections.unmodifiableMap(this.store);
    }

    protected Map<Id, BackendEntry> queryPrefixWith(byte prefix) {
        String prefixString = String.format("%x", prefix);
        Map<Id, BackendEntry> entries = new HashMap<>();
        for (BackendEntry item : this.store.values()) {
            if (item.id().asString().startsWith(prefixString)) {
                entries.put(item.id(), item);
            }
        }
        return entries;
    }

    protected Map<Id, BackendEntry> queryById(Set<Id> ids,
                                              Map<Id, BackendEntry> entries) {
        assert ids.size() > 0;
        Map<Id, BackendEntry> rs = new HashMap<>();

        for (Id id : ids) {
            if (entries.containsKey(id)) {
                rs.put(id, entries.get(id));
            }
        }
        return rs;
    }

    private Map<Id, BackendEntry> queryEdgeById(Set<Id> ids,
                                                Map<Id, BackendEntry> entries) {
        assert ids.size() > 0;
        Map<Id, BackendEntry> rs = new HashMap<>();

        for (Id id : ids) {
            // TODO: improve id split
            String[] parts = SplicingIdGenerator.split(id);
            Id entryId = IdGenerator.of(parts[0]);

            String column = null;
            if (parts.length > 1) {
                parts = Arrays.copyOfRange(parts, 1, parts.length);
                column = String.join(TextBackendEntry.COLUME_SPLITOR, parts);
            } else {
                // All edges
                assert parts.length == 1;
            }

            if (entries.containsKey(entryId)) {
                BackendEntry entry = entries.get(entryId);
                // TODO: Compatible with BackendEntry
                TextBackendEntry textEntry = (TextBackendEntry) entry;
                if (column == null) {
                    // All edges in the vertex
                    rs.put(entryId, entry);
                } else if (textEntry.containsPrefix(column)) {
                    // An edge in the vertex
                    TextBackendEntry result = new TextBackendEntry(entryId);
                    result.columns(textEntry.columnsWithPrefix(column));
                    rs.put(entryId, result);
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

    private static boolean matchCondition(BackendEntry item, Condition c) {
        // TODO: Compatible with BackendEntry
        TextBackendEntry entry = (TextBackendEntry) item;

        // Not supported by memory
        if (!(c instanceof Condition.Relation)) {
            throw new BackendException("Unsupported condition: " + c);
        }

        Condition.Relation r = (Condition.Relation) c;
        String key = r.key().toString();

        // TODO: deal with others Relation like: <, >=, ...
        if (r.relation() == Condition.RelationType.CONTAINS_KEY) {
            return entry.contains(r.value().toString());
        } else if (r.relation() == Condition.RelationType.EQ) {
            return entry.contains(key, r.value().toString());
        } else if (entry.contains(key)) {
            return r.test(entry.column(key));
        }
        return false;
    }

    @Override
    public void mutate(BackendMutation mutation) {
        for (List<MutateItem> items : mutation.mutation().values()) {
            for (MutateItem item : items) {
                mutate(item);
            }
        }
    }

    private void mutate(MutateItem item) {
        BackendEntry entry = item.entry();
        switch (item.action()) {
            case INSERT:
                logger.info("[store {}] add entry: {}", this.name, entry);
                if (!this.store.containsKey(entry.id())) {
                    this.store.put(entry.id(), entry);
                } else {
                    // Merge columns if the entry exists
                    BackendEntry old =  this.store.get(entry.id());
                    // TODO: Compatible with BackendEntry
                    ((TextBackendEntry) old).merge((TextBackendEntry) entry);
                }
                break;
            case DELETE:
                logger.info("[store {}] remove id: {}", this.name, entry.id());
                // Remove by id (TODO: support remove by id + condition)
                this.store.remove(entry.id());
                break;
            case APPEND:
                // Append entry to a column
                BackendEntry parent = this.store.get(entry.id());
                if (parent == null) {
                    this.store.put(entry.id(), entry);
                } else {
                    // TODO: Compatible with BackendEntry
                    ((TextBackendEntry) parent).append((TextBackendEntry) entry);
                }
                break;
            case ELIMINATE:
                // Eliminate item from a column in entry
                parent = this.store.get(entry.id());
                // TODO: Compatible with BackendEntry
                ((TextBackendEntry) parent).eliminate((TextBackendEntry) entry);
                break;
            default:
                throw new BackendException("Unsupported mutate type: %s",
                                           item.action());
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public void open(HugeConfig config) {
        logger.info("open()");
    }

    @Override
    public void close() throws BackendException {
        logger.info("close()");
    }

    @Override
    public void init() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clear() {
        logger.info("clear()");
        this.store.clear();
    }

    @Override
    public void beginTx() {
        // TODO Auto-generated method stub
    }

    @Override
    public void commitTx() {
        // TODO Auto-generated method stub
    }

    @Override
    public void rollbackTx() {
        // TODO Auto-generated method stub
    }

    @Override
    public String toString() {
        return this.name;
    }
}
