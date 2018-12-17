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
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdPrefixQuery;
import com.baidu.hugegraph.backend.query.IdRangeQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.serializer.BinaryEntryIterator;
import com.baidu.hugegraph.backend.serializer.BinaryEntryIterator.PageState;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

public class RocksDBTable extends BackendTable<Session, BackendEntry> {

    private static final Logger LOG = Log.logger(RocksDBStore.class);

    private final RocksDBShardSpliter shardSpliter;

    public RocksDBTable(String database, String table) {
        super(String.format("%s+%s", database, table));
        this.shardSpliter = new RocksDBShardSpliter(this.table());
    }

    @Override
    protected void registerMetaHandlers() {
        this.registerMetaHandler("splits", (session, meta, args) -> {
            E.checkArgument(args.length == 1,
                            "The args count of %s must be 1", meta);
            long splitSize = (long) args[0];
            return this.shardSpliter.getSplits(session, splitSize);
        });
    }

    @Override
    public void init(Session session) {
        // pass
    }

    @Override
    public void clear(Session session) {
        // pass
    }

    @Override
    public void insert(Session session, BackendEntry entry) {
        assert !entry.columns().isEmpty();
        for (BackendColumn col : entry.columns()) {
            assert entry.belongToMe(col) : entry;
            session.put(this.table(), col.name, col.value);
        }
    }

    @Override
    public void delete(Session session, BackendEntry entry) {
        if (entry.columns().isEmpty()) {
            session.delete(this.table(), entry.id().asBytes());
        } else {
            for (BackendColumn col : entry.columns()) {
                assert entry.belongToMe(col) : entry;
                session.remove(this.table(), col.name);
            }
        }
    }

    @Override
    public void append(Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.insert(session, entry);
    }

    @Override
    public void eliminate(Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.delete(session, entry);
    }

    @Override
    public Iterator<BackendEntry> query(Session session, Query query) {
        if (query.limit() == 0 && query.limit() != Query.NO_LIMIT) {
            LOG.debug("Return empty result(limit=0) for query {}", query);
            return ImmutableList.<BackendEntry>of().iterator();
        }

        // Query all
        if (query.empty()) {
            return newEntryIterator(this.queryAll(session, query), query);
        }

        // Query by prefix
        if (query instanceof IdPrefixQuery) {
            IdPrefixQuery pq = (IdPrefixQuery) query;
            return newEntryIterator(this.queryByPrefix(session, pq), query);
        }

        // Query by range
        if (query instanceof IdRangeQuery) {
            IdRangeQuery rq = (IdRangeQuery) query;
            return newEntryIterator(this.queryByRange(session, rq), query);
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
        ConditionQuery cq = (ConditionQuery) query;
        return newEntryIterator(this.queryByCond(session, cq), query);
    }

    protected BackendColumnIterator queryAll(Session session, Query query) {
        if (query.paging()) {
            PageState page = PageState.fromString(query.page());
            byte[] begin = page.position();
            return session.scan(this.table(), begin, null, Session.SCAN_ANY);
        } else {
            return session.scan(this.table());
        }
    }

    protected BackendColumnIterator queryById(Session session, Id id) {
        // TODO: change to get() after vertex and schema don't use id prefix
        return session.scan(this.table(), id.asBytes());
    }

    protected BackendColumnIterator queryByPrefix(Session session,
                                                  IdPrefixQuery query) {
        int type = query.inclusiveStart() ?
                   Session.SCAN_GTE_BEGIN : Session.SCAN_GT_BEGIN;
        type |= Session.SCAN_PREFIX_END;
        return session.scan(this.table(), query.start().asBytes(),
                            query.prefix().asBytes(), type);
    }

    protected BackendColumnIterator queryByRange(Session session,
                                                 IdRangeQuery query) {
        byte[] start = query.start().asBytes();
        byte[] end = query.end() == null ? null : query.end().asBytes();
        int type = query.inclusiveStart() ?
                   Session.SCAN_GTE_BEGIN : Session.SCAN_GT_BEGIN;
        if (end != null) {
            type |= query.inclusiveEnd() ?
                    Session.SCAN_LTE_END : Session.SCAN_LT_END;
        }
        return session.scan(this.table(), start, end, type);
    }

    protected BackendColumnIterator queryByCond(Session session,
                                                ConditionQuery query) {
        if (query.containsScanCondition()) {
            E.checkArgument(query.relations().size() == 1,
                            "Invalid scan with multi conditions: %s", query);
            Relation scan = query.relations().iterator().next();
            Shard shard = (Shard) scan.value();
            return this.queryByRange(session, shard);
        }
        throw new NotSupportException("query: %s", query);
    }

    protected BackendColumnIterator queryByRange(Session session, Shard shard) {
        byte[] start = this.shardSpliter.position(shard.start());
        byte[] end = this.shardSpliter.position(shard.end());
        return session.scan(this.table(), start, end);
    }

    protected static BackendEntryIterator newEntryIterator(
                                          BackendColumnIterator cols,
                                          Query query) {
        return new BinaryEntryIterator<BackendColumn>(cols, query,
                                                      (entry, col) -> {
            if (entry == null || !entry.belongToMe(col)) {
                HugeType type = query.resultType();
                // NOTE: only support BinaryBackendEntry currently
                entry = new BinaryBackendEntry(type, col.name);
            }
            entry.columns(col);
            return entry;
        });
    }

    private static class RocksDBShardSpliter extends ShardSpliter<Session> {

        private static final String MEM_SIZE = "rocksdb.size-all-mem-tables";
        private static final String SST_SIZE = "rocksdb.total-sst-files-size";

        private static final String NUM_KEYS = "rocksdb.estimate-num-keys";

        public RocksDBShardSpliter(String table) {
            super(table);
        }

        @Override
        public long estimateDataSize(Session session) {
            long mem = Long.parseLong(session.property(this.table(), MEM_SIZE));
            long sst = Long.parseLong(session.property(this.table(), SST_SIZE));
            return mem + sst;
        }

        @Override
        public long estimateNumKeys(Session session) {
            return Long.parseLong(session.property(this.table(), NUM_KEYS));
        }
    }
}
