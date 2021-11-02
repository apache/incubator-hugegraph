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

package com.baidu.hugegraph.backend.store.mysql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.StringEncoding;

public class MysqlEntryIterator extends BackendEntryIterator {

    private final ResultSetWrapper results;
    private final BiFunction<BackendEntry, BackendEntry, BackendEntry> merger;

    private BackendEntry next;
    private BackendEntry lastest;
    private boolean exceedLimit;

    public MysqlEntryIterator(ResultSetWrapper rs, Query query,
           BiFunction<BackendEntry, BackendEntry, BackendEntry> merger) {
        super(query);
        this.results = rs;
        this.merger = merger;
        this.next = null;
        this.lastest = null;
        this.exceedLimit = false;
    }

    @Override
    protected final boolean fetch() {
        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }

        try {
            while (!this.results.isClosed() && this.results.next()) {
                MysqlBackendEntry entry = this.row2Entry(this.results.resultSet());
                this.lastest = entry;
                BackendEntry merged = this.merger.apply(this.current, entry);
                if (this.current == null) {
                    // The first time to read
                    this.current = merged;
                } else if (merged == this.current) {
                    // Does the next entry belongs to the current entry
                    assert merged != null;
                } else {
                    // New entry
                    assert this.next == null;
                    this.next = merged;
                    break;
                }

                // When limit exceed, stop fetching
                if (this.reachLimit(this.fetched() - 1)) {
                    this.exceedLimit = true;
                    // Need remove last one because fetched limit + 1 records
                    this.removeLastRecord();
                    this.results.close();
                    break;
                }
            }
        } catch (SQLException e) {
            throw new BackendException("Fetch next error", e);
        }
        return this.current != null;
    }

    @Override
    protected PageState pageState() {
        byte[] position;
        // There is no latest or no next page
        if (this.lastest == null || !this.exceedLimit &&
            this.fetched() <= this.query.limit() && this.next == null) {
            position = PageState.EMPTY_BYTES;
        } else {
            MysqlBackendEntry entry = (MysqlBackendEntry) this.lastest;
            position = new PagePosition(entry.columnsMap()).toBytes();
        }
        return new PageState(position, 0, (int) this.count());
    }

    @Override
    protected void skipOffset() {
        // pass
    }

    @Override
    protected final long sizeOf(BackendEntry entry) {
        MysqlBackendEntry e = (MysqlBackendEntry) entry;
        int subRowsSize = e.subRows().size();
        return subRowsSize > 0 ? subRowsSize : 1L;
    }

    @Override
    protected final long skip(BackendEntry entry, long skip) {
        MysqlBackendEntry e = (MysqlBackendEntry) entry;
        E.checkState(e.subRows().size() > skip, "Invalid entry to skip");
        for (long i = 0; i < skip; i++) {
            e.subRows().remove(0);
        }
        return e.subRows().size();
    }

    @Override
    public void close() throws Exception {
        this.results.close();
    }

    private MysqlBackendEntry row2Entry(ResultSet result) throws SQLException {
        HugeType type = this.query.resultType();
        MysqlBackendEntry entry = new MysqlBackendEntry(type);
        ResultSetMetaData metaData = result.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String name = metaData.getColumnLabel(i);
            HugeKeys key = MysqlTable.parseKey(name);
            Object value = result.getObject(i);
            if (value == null) {
                assert key == HugeKeys.EXPIRED_TIME;
                continue;
            }
            entry.column(key, value);
        }
        return entry;
    }

    private void removeLastRecord() {
        MysqlBackendEntry entry = (MysqlBackendEntry) this.current;
        int lastOne = entry.subRows().size() - 1;
        assert lastOne >= 0;
        entry.subRows().remove(lastOne);
    }

    public static class PagePosition {

        private final Map<HugeKeys, Object> columns;

        public PagePosition(Map<HugeKeys, Object> columns) {
            this.columns = columns;
        }

        public Map<HugeKeys, Object> columns() {
            return this.columns;
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this.columns);
        }

        public byte[] toBytes() {
            String json = JsonUtil.toJson(this.columns);
            return StringEncoding.encode(json);
        }

        public static PagePosition fromBytes(byte[] bytes) {
            String json = StringEncoding.decode(bytes);
            @SuppressWarnings("unchecked")
            Map<String, Object> columns = JsonUtil.fromJson(json, Map.class);
            Map<HugeKeys, Object> keyColumns = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : columns.entrySet()) {
                HugeKeys key = MysqlTable.parseKey(entry.getKey());
                keyColumns.put(key, entry.getValue());
            }
            return new PagePosition(keyColumns);
        }
    }
}
