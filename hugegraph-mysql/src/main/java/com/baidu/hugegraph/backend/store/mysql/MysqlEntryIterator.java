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

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;

public class MysqlEntryIterator extends BackendEntryIterator<ResultSet> {

    private final ResultSet results;
    private final BiFunction<BackendEntry, BackendEntry, BackendEntry> merger;

    private BackendEntry next;
    private BackendEntry last;

    public MysqlEntryIterator(ResultSet rs, Query query,
           BiFunction<BackendEntry, BackendEntry, BackendEntry> merger) {
        super(query);
        this.results = rs;
        this.merger = merger;
        this.next = null;
        this.last = null;
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
                MysqlBackendEntry e = this.row2Entry(this.results);
                this.last = e;
                BackendEntry merged = this.merger.apply(this.current, e);
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
                    return true;
                }
            }
        } catch (SQLException e) {
            throw new BackendException("Fetch next error", e);
        }
        return this.current != null;
    }

    @Override
    protected String pageState() {
        if (this.last == null) {
            return null;
        }
        if (this.fetched() <= query.limit() && this.next == null) {
            // There is no next page
            return null;
        }
        MysqlBackendEntry entry = (MysqlBackendEntry) this.last;
        PageState pageState = new PageState(entry.columnsMap());
        return pageState.toString();
    }

    @Override
    protected final long sizeOf(BackendEntry entry) {
        MysqlBackendEntry e = (MysqlBackendEntry) entry;
        int subRowsSize = e.subRows().size();
        return subRowsSize > 0 ? subRowsSize : 1L;
    }

    @Override
    protected final long offset() {
        return 0L;
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

    private MysqlBackendEntry row2Entry(ResultSet result) throws SQLException {
        HugeType type = this.query.resultType();
        MysqlBackendEntry entry = new MysqlBackendEntry(type);
        ResultSetMetaData metaData = result.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String name = metaData.getColumnLabel(i);
            Object value = result.getObject(i);
            entry.column(MysqlTable.parseKey(name), value);
        }
        return entry;
    }

    @Override
    public void close() throws Exception {
        this.results.close();
    }

    public static class PageState {

        private static final String CHARSET = "utf-8";
        private final Map<HugeKeys, Object> columns;

        public PageState(Map<HugeKeys, Object> columns) {
            this.columns = columns;
        }

        public Map<HugeKeys, Object> columns() {
            return this.columns;
        }

        @Override
        public String toString() {
            return Base64.getEncoder().encodeToString(this.toBytes());
        }

        public byte[] toBytes() {
            String json = JsonUtil.toJson(this.columns);
            try {
                return json.getBytes(CHARSET);
            } catch (UnsupportedEncodingException e) {
                throw new BackendException(e);
            }
        }

        public static PageState fromString(String page) {
            byte[] bytes;
            try {
                bytes = Base64.getDecoder().decode(page);
            } catch (Exception e) {
                throw new BackendException("Invalid page: '%s'", e, page);
            }
            return fromBytes(bytes);
        }

        private static PageState fromBytes(byte[] bytes) {
            String json;
            try {
                json = new String(bytes, CHARSET);
            } catch (UnsupportedEncodingException e) {
                throw new BackendException(e);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> columns = JsonUtil.fromJson(json, Map.class);
            Map<HugeKeys, Object> keyColumns = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : columns.entrySet()) {
                HugeKeys key = MysqlTable.parseKey(entry.getKey());
                keyColumns.put(key, entry.getValue());
            }
            return new PageState(keyColumns);
        }
    }
}
