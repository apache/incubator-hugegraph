/*
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

package org.apache.hugegraph.backend.store.hstore;

import java.util.List;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.Condition.Relation;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.hstore.HstoreSessions.Session;
import org.apache.hugegraph.type.HugeTableType;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;

public class HstoreTables {

    public static class Vertex extends HstoreTable {

        public static final String TABLE = HugeTableType.VERTEX.string();

        public Vertex(String database) {
            super(database, TABLE);
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }
    }

    /**
     * task信息存储表
     */
    public static class TaskInfo extends HstoreTable {

        public static final String TABLE = HugeTableType.TASK_INFO_TABLE.string();

        public TaskInfo(String database) {
            super(database, TABLE);
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }
    }

    public static class ServerInfo extends HstoreTable {

        public static final String TABLE = HugeTableType.SERVER_INFO_TABLE.string();

        public ServerInfo(String database) {
            super(database, TABLE);
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }
    }

    public static class Edge extends HstoreTable {

        public static final String TABLE_SUFFIX = HugeType.EDGE.string();

        public Edge(boolean out, String database) {
            // Edge out/in table
            super(database, (out ? HugeTableType.OUT_EDGE.string() :
                             HugeTableType.IN_EDGE.string()));
        }

        public static Edge out(String database) {
            return new Edge(true, database);
        }

        public static Edge in(String database) {
            return new Edge(false, database);
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }
    }

    public static class IndexTable extends HstoreTable {

        public static final String TABLE = HugeTableType.ALL_INDEX_TABLE.string();

        public IndexTable(String database) {
            super(database, TABLE);
        }

        @Override
        public void eliminate(Session session, BackendEntry entry) {
            assert entry.columns().size() == 1;
            super.delete(session, entry);
        }

        @Override
        public void delete(Session session, BackendEntry entry) {
            /*
             * Only delete index by label will come here
             * Regular index delete will call eliminate()
             */
            byte[] ownerKey = super.ownerDelegate.apply(entry);
            for (BackendEntry.BackendColumn column : entry.columns()) {
                // Don't assert entry.belongToMe(column), length-prefix is 1*
                session.deletePrefix(this.table(), ownerKey, column.name);
            }
        }

        /**
         * 主要用于 range类型的index处理
         *
         * @param session
         * @param query
         * @return
         */
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
                BinarySerializer.increaseOne(begin);
            }
            byte[] ownerStart = this.ownerScanDelegate.get();
            byte[] ownerEnd = this.ownerScanDelegate.get();
            if (max == null) {
                E.checkArgumentNotNull(prefix, "Range index prefix is missing");
                return session.scan(this.table(), ownerStart, ownerEnd, begin,
                                    prefix.asBytes(), Session.SCAN_PREFIX_END);
            } else {
                byte[] end = max.asBytes();
                int type = maxEq ? Session.SCAN_LTE_END : Session.SCAN_LT_END;
                return session.scan(this.table(), ownerStart,
                                    ownerEnd, begin, end, type);
            }
        }
    }

    public static class OlapTable extends HstoreTable {

        public static final String TABLE = HugeTableType.OLAP_TABLE.string();

        public OlapTable(String database) {
            // 由原先多个ap_{pk_id} 合并成一个ap表
            super(database, TABLE);
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }

        @Override
        public boolean isOlap() {
            return true;
        }
    }
}
