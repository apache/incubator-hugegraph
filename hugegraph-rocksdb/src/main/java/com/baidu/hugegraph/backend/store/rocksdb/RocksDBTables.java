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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;

public class RocksDBTables {

    public static class Counters extends RocksDBTable {

        private static final String TABLE = "c";

        private static final int MAX_TIMES = 1000;
        private static final byte[] ONE = b(1L);

        public Counters(String database) {
            super(database, TABLE);
        }

        public synchronized Id nextId(Session session, HugeType type) {
            byte[] key = new byte[]{type.code()};

            // Do get-increase-get-compare operation
            long counter = 0L;
            long expect = -1L;
            for (int i = 0; i < MAX_TIMES; i++) {
                // Get the latest value
                byte[] value = session.get(this.table(), key);
                if (value != null) {
                    counter = l(value);
                }
                if (counter == expect) {
                    break;
                }
                // Increase local counter
                expect = counter + 1L;
                // Increase 1, the default value of counter is 0 in RocksDB
                session.merge(this.table(), key, ONE);
                session.commit();
            }

            E.checkState(counter != 0L, "Please check whether RocksDB is OK");
            E.checkState(counter == expect, "RocksDB is busy please try again");
            return IdGenerator.of(counter);
        }

        private static byte[] b(long value) {
            return ByteBuffer.allocate(Long.BYTES)
                             .order(ByteOrder.nativeOrder())
                             .putLong(value).array();
        }

        private static long l(byte[] bytes) {
            assert bytes.length == Long.BYTES;
            return ByteBuffer.wrap(bytes)
                             .order(ByteOrder.nativeOrder())
                             .getLong();
        }
    }

    public static class VertexLabel extends RocksDBTable {

        public static final String TABLE = "vl";

        public VertexLabel(String database) {
            super(database, TABLE);
        }
    }

    public static class EdgeLabel extends RocksDBTable {

        public static final String TABLE = "el";

        public EdgeLabel(String database) {
            super(database, TABLE);
        }
    }

    public static class PropertyKey extends RocksDBTable {

        public static final String TABLE = "pk";

        public PropertyKey(String database) {
            super(database, TABLE);
        }
    }

    public static class IndexLabel extends RocksDBTable {

        public static final String TABLE = "il";

        public IndexLabel(String database) {
            super(database, TABLE);
        }
    }

    public static class Vertex extends RocksDBTable {

        public static final String TABLE = "v";

        public Vertex(String database) {
            super(database, TABLE);
        }
    }

    public static class Edge extends RocksDBTable {

        public static final String TABLE_SUFFIX = "e";

        public Edge(boolean out, String database) {
            // Edge out/in table
            super(database, (out ? 'o' : 'i') + TABLE_SUFFIX);
        }

        public static Edge out(String database) {
            return new Edge(true, database);
        }

        public static Edge in(String database) {
            return new Edge(false, database);
        }
    }

    public static class SecondaryIndex extends RocksDBTable {

        public static final String TABLE = "si";

        public SecondaryIndex(String database) {
            this(database, TABLE);
        }

        protected SecondaryIndex(String database, String table) {
            super(database, table);
        }

        @Override
        public void delete(Session session, BackendEntry entry) {
            /*
             * Only delete index by label will come here
             * Regular index delete will call eliminate()
             */
            for (BackendEntry.BackendColumn column : entry.columns()) {
                // Don't assert entry.belongToMe(column), length-prefix is 1*
                session.delete(this.table(), column.name);
            }
        }
    }

    public static class SearchIndex extends SecondaryIndex {

        public static final String TABLE = "fi";

        public SearchIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class RangeIndex extends RocksDBTable {

        public static final String TABLE = "ri";

        public RangeIndex(String database) {
            super(database, TABLE);
        }

        @Override
        protected BackendColumnIterator queryByCond(Session session,
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
            if (!minEq) {
                begin = RocksDBStdSessions.increase(begin);
            }

            if (max == null) {
                E.checkArgumentNotNull(prefix, "Range index prefix is missing");
                return session.scan(this.table(), begin, prefix.asBytes(),
                                    Session.SCAN_PREFIX_WITH_END);
            } else {
                byte[] end = max.asBytes();
                int type = maxEq ? Session.SCAN_LTE_END : Session.SCAN_LT_END;
                return session.scan(this.table(), begin, end, type);
            }
        }

        @Override
        public void delete(Session session, BackendEntry entry) {
            /*
             * Only delete index by label will come here
             * Regular index delete will call eliminate()
             */
            for (BackendEntry.BackendColumn column : entry.columns()) {
                session.delete(this.table(), column.name);
            }
        }
    }
}
