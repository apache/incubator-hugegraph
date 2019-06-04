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
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.serializer.BinaryEntryIterator;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.backend.store.hbase.HbaseSessions.RowIterator;
import com.baidu.hugegraph.backend.store.hbase.HbaseSessions.Session;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;

public class HbaseTables {

    public static class Counters extends HbaseTable {

        private static final String TABLE = "c";
        private static final byte[] COL = Bytes.toBytes("c");

        public Counters() {
            super(TABLE);
        }

        public long getCounter(Session session, HugeType type) {
            byte[] key = new byte[]{type.code()};
            RowIterator results = session.get(this.table(), CF, key);
            if (results.hasNext()) {
                Result row = results.next();
                return NumericUtil.bytesToLong(row.getValue(CF, COL));
            } else {
                return 0L;
            }
        }

        public void increaseCounter(Session session, HugeType type,
                                    long increment) {
            byte[] key = new byte[]{type.code()};
            session.increase(this.table(), CF, key, COL, increment);
        }
    }

    public static class VertexLabel extends HbaseTable {

        public static final String TABLE = "vl";

        public VertexLabel() {
            super(TABLE);
        }
    }

    public static class EdgeLabel extends HbaseTable {

        public static final String TABLE = "el";

        public EdgeLabel() {
            super(TABLE);
        }
    }

    public static class PropertyKey extends HbaseTable {

        public static final String TABLE = "pk";

        public PropertyKey() {
            super(TABLE);
        }
    }

    public static class IndexLabel extends HbaseTable {

        public static final String TABLE = "il";

        public IndexLabel() {
            super(TABLE);
        }
    }

    public static class Vertex extends HbaseTable {

        public static final String TABLE = "v";

        public Vertex(String store) {
            super(joinTableName(store, TABLE));
        }
    }

    public static class Edge extends HbaseTable {

        public static final String TABLE_SUFFIX = "e";

        public Edge(String store, boolean out) {
            super(joinTableName(store, table(out)));
        }

        private static String table(boolean out) {
            // Edge out/in table
            return (out ? 'o' : 'i') + TABLE_SUFFIX;
        }

        public static Edge out(String store) {
            return new Edge(store, true);
        }

        public static Edge in(String store) {
            return new Edge(store, false);
        }

        @Override
        public void insert(Session session, BackendEntry entry) {
            for (BackendColumn col : entry.columns()) {
                E.checkArgument(col.name.length == 0,
                                "Expect empty column name, " +
                                "please ensure hbase serializer is used");
            }
            super.insert(session, entry);
        }

        @Override
        protected void parseRowColumns(Result row, BackendEntry entry,
                                       Query query) throws IOException {
            /*
             * Collapse owner-vertex id from edge id, NOTE: unneeded to
             * collapse if BinarySerializer.keyWithIdPrefix set to true
             */
            byte[] key = row.getRow();
            key = Arrays.copyOfRange(key, entry.id().length(), key.length);

            long total = query.total();
            CellScanner cellScanner = row.cellScanner();
            while (cellScanner.advance() && total-- > 0) {
                Cell cell = cellScanner.current();
                assert CellUtil.cloneQualifier(cell).length == 0;
                entry.columns(BackendColumn.of(key, CellUtil.cloneValue(cell)));
            }
        }
    }

    public static class IndexTable extends HbaseTable {

        private static final int INDEX_DELETE_BATCH = 1000;

        public IndexTable(String table) {
            super(table);
        }

        @Override
        public void insert(Session session, BackendEntry entry) {
            assert entry.columns().size() == 1;
            BackendColumn col = entry.columns().iterator().next();
            session.put(this.table(), CF, col.name,
                        BinarySerializer.EMPTY_BYTES, col.value);
        }

        @Override
        public void eliminate(Session session, BackendEntry entry) {
            assert entry.columns().size() == 1;
            BackendColumn col = entry.columns().iterator().next();
            session.delete(this.table(), CF, col.name);
        }

        @Override
        public void delete(Session session, BackendEntry entry) {
            /*
             * Only delete index by label will come here
             * Regular index delete will call eliminate()
             */
            int count = 0;
            for (BackendColumn column : entry.columns()) {
                session.commit();
                // Prefix query index label related indexes
                RowIterator iter = session.scan(this.table(), column.name);
                while (iter.hasNext()) {
                    session.delete(this.table(), CF, iter.next().getRow());
                    // Commit once reaching batch size
                    if (++count >= INDEX_DELETE_BATCH) {
                        session.commit();
                        count = 0;
                    }
                }
            }
            if (count > 0) {
                session.commit();
            }
        }

        @Override
        protected BackendEntryIterator newEntryIterator(RowIterator rows,
                                                        Query query) {
            return new BinaryEntryIterator<>(rows, query, (entry, row) -> {
                assert row.size() == 1;
                BackendColumn col = BackendColumn.of(row.getRow(), row.value());
                entry = new BinaryBackendEntry(query.resultType(), col.name);
                entry.columns(col);
                return entry;
            });
        }
    }

    public static class SecondaryIndex extends IndexTable {

        public static final String TABLE = "si";

        public SecondaryIndex(String store) {
            super(joinTableName(store, TABLE));
        }
    }

    public static class SearchIndex extends IndexTable {

        public static final String TABLE = "fi";

        public SearchIndex(String store) {
            super(joinTableName(store, TABLE));
        }
    }

    public static class RangeIndex extends IndexTable {

        public static final String TABLE = "ri";

        public RangeIndex(String store) {
            super(joinTableName(store, TABLE));
        }
    }
}
