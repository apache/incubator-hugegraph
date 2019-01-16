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

package com.baidu.hugegraph.backend.store.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdPrefixQuery;
import com.baidu.hugegraph.backend.query.IdRangeQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.serializer.BinaryEntryIterator;
import com.baidu.hugegraph.backend.serializer.BinaryEntryIterator.PageState;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.backend.store.hbase.HbaseSessions.RowIterator;
import com.baidu.hugegraph.backend.store.hbase.HbaseSessions.Session;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

public class HbaseTable extends BackendTable<Session, BackendEntry> {

    private static final Logger LOG = Log.logger(HbaseStore.class);

    protected static final byte[] CF = "f".getBytes();

    private final HbaseShardSpliter shardSpliter;

    public HbaseTable(String table) {
        super(table);
        this.shardSpliter = new HbaseShardSpliter(this.table());
    }

    public static List<byte[]> cfs() {
        return ImmutableList.of(CF);
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
        session.put(this.table(), CF, entry.id().asBytes(), entry.columns());
    }

    @Override
    public void delete(Session session, BackendEntry entry) {
        if (entry.columns().isEmpty()) {
            session.delete(this.table(), CF, entry.id().asBytes());
        } else {
            for (BackendColumn col : entry.columns()) {
                session.remove(table(), CF, entry.id().asBytes(), col.name);
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
            RowIterator rowIterator = null;
            if (query.ids().size() == 1) {
                Id id = query.ids().iterator().next();
                rowIterator = this.queryById(session, id);
            } else {
                rowIterator = this.queryByIds(session, query.ids());
            }
            return newEntryIterator(rowIterator, query);
        }

        // Query by condition (or condition + id)
        ConditionQuery cq = (ConditionQuery) query;
        return newEntryIterator(this.queryByCond(session, cq), query);
    }

    protected RowIterator queryAll(Session session, Query query) {
        if (query.paging()) {
            PageState page = PageState.fromString(query.page());
            byte[] begin = page.position();
            return session.scan(this.table(), begin, null);
        } else {
            return session.scan(this.table(), -1);
        }
    }

    protected RowIterator queryById(Session session, Id id) {
        return session.get(this.table(), null, id.asBytes());
    }

    protected RowIterator queryByIds(Session session, Set<Id> ids) {
        Set<byte[]> rowkeys = InsertionOrderUtil.newSet();
        for (Id id : ids) {
            rowkeys.add(id.asBytes());
        }
        return session.get(this.table(), null, rowkeys);
    }

    protected RowIterator queryByPrefix(Session session, IdPrefixQuery query) {
        return session.scan(this.table(), query.start().asBytes(),
                            query.inclusiveStart(), query.prefix().asBytes());
    }

    protected RowIterator queryByRange(Session session, IdRangeQuery query) {
        byte[] start = query.start().asBytes();
        byte[] end = query.end() == null ? null : query.end().asBytes();
        if (query.inclusiveEnd() && end != null) {
            BinarySerializer.increaseOne(end);
            return session.scan(this.table(), start, query.inclusiveStart(),
                                end, false);
        }
        return session.scan(this.table(), start, query.inclusiveStart(),
                            end, query.inclusiveEnd());
    }

    protected RowIterator queryByCond(Session session, ConditionQuery query) {
        if (query.containsScanCondition()) {
            E.checkArgument(query.relations().size() == 1,
                            "Invalid scan with multi conditions: %s", query);
            Relation scan = query.relations().iterator().next();
            Shard shard = (Shard) scan.value();
            return this.queryByRange(session, shard);
        }
        throw new NotSupportException("query: %s", query);
    }

    protected RowIterator queryByRange(Session session, Shard shard) {
        byte[] start = this.shardSpliter.position(shard.start());
        byte[] end = this.shardSpliter.position(shard.end());
        return session.scan(this.table(), start, end);
    }

    protected BackendEntryIterator newEntryIterator(RowIterator rows,
                                                    Query query) {
        return new BinaryEntryIterator<>(rows, query, (entry, row) -> {
            E.checkState(!row.isEmpty(), "Can't parse empty HBase result");
            byte[] id = row.getRow();
            if (entry == null || !Bytes.prefixWith(id, entry.id().asBytes())) {
                HugeType type = query.resultType();
                // NOTE: only support BinaryBackendEntry currently
                entry = new BinaryBackendEntry(type, id);
            }
            try {
                this.parseRowColumns(row, entry, query);
            } catch (IOException e) {
                throw new BackendException("Failed to read HBase columns", e);
            }
            return entry;
        });
    }

    protected void parseRowColumns(Result row, BackendEntry entry, Query query)
                                   throws IOException {
        CellScanner cellScanner = row.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            entry.columns(BackendColumn.of(CellUtil.cloneQualifier(cell),
                                           CellUtil.cloneValue(cell)));
        }
    }

    private static class HbaseShardSpliter extends ShardSpliter<Session> {

        public HbaseShardSpliter(String table) {
            super(table);
        }

        @Override
        public long estimateDataSize(Session session) {
            try {
                return session.storeSize(this.table());
            } catch (IOException ignored) {
                return -1L;
            }
        }

        @Override
        public long estimateNumKeys(Session session) {
            // TODO: improve
            return 100000L;
        }
    }
}
