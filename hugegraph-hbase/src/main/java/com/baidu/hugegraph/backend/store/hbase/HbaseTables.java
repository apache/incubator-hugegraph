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
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.hbase.HbaseSessions.RowIterator;
import com.baidu.hugegraph.backend.store.hbase.HbaseSessions.Session;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;

public class HbaseTables {

    public static class Counters extends HbaseTable {

        private static final String TABLE = "c";
        private static final byte[] COL = Bytes.toBytes("c");

        public Counters() {
            super(TABLE);
        }

        public synchronized Id nextId(Session session, HugeType type) {
            byte[] key = new byte[]{type.code()};
            long counter = session.increase(this.table(), CF, key, COL, 1L);
            return IdGenerator.of(counter);
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
        protected RowIterator queryById(Session session, Id id) {
            byte[] prefix = id.asBytes();
            return session.scan(this.table(), prefix);
        }

        @Override
        protected void parseRowColumns(Result row, BackendEntry entry)
                                       throws IOException {
            byte[] key = row.getRow();
            // Collapse owner vertex id
            key = Arrays.copyOfRange(key, entry.id().length(), key.length);

            CellScanner cellScanner = row.cellScanner();
            while (cellScanner.advance()) {
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
        public void delete(Session session, BackendEntry entry) {
            /*
             * Only delete index by label will come here
             * Regular index delete will call eliminate()
             */
            int count = 0;
            for (BackendColumn column : entry.columns()) {
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
    }

    public static class SecondaryIndex extends IndexTable {

        public static final String TABLE = "si";

        public SecondaryIndex(String store) {
            super(joinTableName(store, TABLE));
        }
    }

    public static class RangeIndex extends IndexTable {

        public static final String TABLE = "ri";

        public RangeIndex(String store) {
            super(joinTableName(store, TABLE));
        }

        @Override
        protected RowIterator queryByCond(Session session,
                                          ConditionQuery query) {
            assert !query.conditions().isEmpty();

            List<Condition> conds = query.syspropConditions(HugeKeys.ID);
            E.checkArgument(!conds.isEmpty(),
                            "Please specify the index conditions");

            Id prefix = null;
            Id min = null;
            boolean minEq = false;
            Id max = null;
            boolean maxEq = false;

            for (Condition c : conds) {
                Relation r = (Relation) c;
                switch (r.relation()) {
                    case PREFIX:
                        prefix = (Id) r.value();
                        break;
                    case GTE:
                        minEq = true;
                    case GT:
                        min = (Id) r.value();
                        break;
                    case LTE:
                        maxEq = true;
                    case LT:
                        max = (Id) r.value();
                        break;
                    default:
                        E.checkArgument(false, "Unsupported relation '%s'",
                                        r.relation());
                }
            }

            E.checkArgumentNotNull(min, "Range index begin key is missing");
            byte[] begin = min.asBytes();
            if (max == null) {
                E.checkArgumentNotNull(prefix, "Range index prefix is missing");
                byte[] prefixFilter = prefix.asBytes();
                return session.scan(this.table(), begin, minEq, prefixFilter);
            } else {
                byte[] end = max.asBytes();
                if (maxEq) {
                    // The parameter stoprow-inclusive doesn't work before v2.0
                    // https://issues.apache.org/jira/browse/HBASE-20675
                    maxEq = false;
                    // Add a trailing 0 byte to stopRow
                    end = Arrays.copyOfRange(end, 0, end.length + 1);
                }
                return session.scan(this.table(), begin, minEq, end, maxEq);
            }
        }
    }
}
