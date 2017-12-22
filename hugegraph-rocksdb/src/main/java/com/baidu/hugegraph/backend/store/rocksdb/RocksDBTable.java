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

package com.baidu.hugegraph.backend.store.rocksdb;

import java.util.Iterator;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.serializer.BinaryEntryIterator;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

public class RocksDBTable {

    private static final Logger LOG = Log.logger(RocksDBStore.class);

    private final String table;

    public RocksDBTable(String database, String table) {
        this.table = String.format("%s/%s", database, table);
    }

    public final String table() {
        return this.table;
    }

    public void insert(Session session, BackendEntry entry) {
        assert !entry.columns().isEmpty();
        for (BackendColumn col : entry.columns()) {
            assert entry.belongToMe(col) : entry;
            session.put(this.table, col.name, col.value);
        }
    }

    public void delete(Session session, BackendEntry entry) {
        if (entry.columns().isEmpty()) {
            session.delete(this.table, entry.id().asBytes());
        } else {
            for (BackendColumn col : entry.columns()) {
                assert entry.belongToMe(col) : entry;
                session.remove(this.table, col.name);
            }
        }
    }

    public void append(Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.insert(session, entry);
    }

    public void eliminate(Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.delete(session, entry);
    }

    public Iterator<BackendEntry> query(Session session, Query query) {
        if (query.limit() == 0 && query.limit() != Query.NO_LIMIT) {
            LOG.debug("Return empty result(limit=0) for query {}", query);
            return ImmutableList.<BackendEntry>of().iterator();
        }

        // Query all
        if (query.empty()) {
            return newEntryIterator(session.scan(this.table), query);
        }

        // Query by id
        if (query.conditions().isEmpty()) {
            assert !query.ids().isEmpty();
            ExtendableIterator<BackendEntry> rs = new ExtendableIterator<>();
            for (Id id : query.ids()) {
                rs.extend(newEntryIterator(this.queryById(session, id), query));
            }
            return rs;
        }

        // Query by condition (or condition + id)
        return this.queryByCond(session, (ConditionQuery) query);
    }

    protected Iterator<BackendColumn> queryById(Session session, Id id) {
        return session.scan(this.table, id.asBytes());
    }

    protected Iterator<BackendColumn> queryByRange(Session session,
                                                   Id begin, Id end) {
        return session.scan(this.table, begin.asBytes(), end.asBytes());
    }

    protected Iterator<BackendEntry> queryByCond(Session session,
                                                 ConditionQuery query) {
        throw new NotSupportException("query: %s", query);
    }

    protected static BinaryEntryIterator newEntryIterator(
                                         Iterator<BackendColumn> cols,
                                         Query query) {
        HugeType t = query.resultType();
        return new BinaryEntryIterator(cols, query, c ->
            // NOTE: only support BinarySerializer currently
            new BinaryBackendEntry(t, BinarySerializer.splitIdKey(t, c.name))
        );
    }
}
