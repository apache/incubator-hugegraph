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

package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessions.Session;
import com.baidu.hugegraph.backend.store.hstore.fake.IdClient;
import com.baidu.hugegraph.backend.store.hstore.fake.IdClientFactory;
import com.baidu.hugegraph.pd.grpc.Pdpb;
import com.baidu.hugegraph.store.term.HgPair;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class HstoreTables {

    public static class Counters extends HstoreTable {

        private static final String TABLE = HugeType.COUNTER.string();
        private static final int DELTA = 10000;
        private static final int times = 10000;
        private static final String DELIMITER="/";
        private static ConcurrentHashMap<String, HgPair> IDS =
                                                 new ConcurrentHashMap<>();


        public Counters(String namespace) {
            super(namespace, TABLE);
        }

        //Get Counter from PD and have a data increment locally,
        // which you can consider persisting to if something unexpected happens
        public long getCounterFromPd(Session session, HugeType type) {
            AtomicLong currentId,maxId;
            HgPair<AtomicLong, AtomicLong> idPair;
            String key = toKey(session.getGraphName(),type);
            if ((idPair = IDS.get(key)) == null) {
                synchronized (IDS) {
                    if ((idPair = IDS.get(key)) == null) {
                        try {
                            currentId = new AtomicLong(0);
                            maxId = new AtomicLong(0);
                            idPair = new HgPair(currentId, maxId);
                            IDS.put(key, idPair);
                        } catch (Exception e) {
                            throw new BackendException(String.format(
                                  "Failed to get the ID from pd,%s", e));
                        }
                    }
                }
            }
            currentId = idPair.getKey();
            maxId = idPair.getValue();
            for (int i = 0; i < times; i++) {
                synchronized (currentId) {
                    if ((currentId.incrementAndGet()) <= maxId.longValue())
                        return currentId.longValue();
                    if (currentId.longValue() > maxId.longValue()) {
                        try {
                            IdClient client= IdClientFactory.getClient(session,
                                                                       this.table());
                            Pdpb.GetIdResponse idByKey =
                                    client.getIdByKey(key, DELTA);
                            idPair.getValue().getAndSet(idByKey.getId() +
                                                        idByKey.getDelta());
                            idPair.getKey().getAndSet(idByKey.getId());
                        } catch (Exception e) {
                            throw new BackendException(String.format(
                                  "Failed to get the ID from pd,%s", e));
                        }
                    }
                }
            }
            E.checkArgument(false,
                            "Having made too many attempts to get the" +
                            " ID for type '%s'",
                            type.name());
            return 0L;
        }

        protected String toKey(String graphName, HugeType type){
            StringBuilder builder = new StringBuilder();
            builder.append(graphName)
                   .append(DELIMITER)
                   .append(type.code());
            return builder.toString();
        }

        public long getCounter(Session session, HugeType type) {
            byte[] key = new byte[] { type.code() };
            byte[] value = session.get(this.table(), COUNTER_OWNER, key);
            if (value.length != 0) {
                return l(value);
            } else {
                return 0L;
            }
        }
        //for Counter to use the specific key
        public  static  final byte[] COUNTER_OWNER = new byte[] { 'c' };

        public synchronized void increaseCounter(Session session,
                                                 HugeType type, long lowest) {
            String key = toKey(session.getGraphName(),type);
            getCounterFromPd(session,type);
            HgPair<AtomicLong,AtomicLong> idPair = IDS.get(key);
            AtomicLong currentId = idPair.getKey();
            AtomicLong maxId = idPair.getValue();
            if (currentId.longValue() >= lowest) return;
            if (maxId.longValue() >= lowest) {
                currentId.set(lowest);
                return;
            }
            synchronized (IDS){
                try{
                    IdClient client= IdClientFactory.getClient(session,
                                                               this.table());
                    client.getIdByKey(key,(int) (lowest - maxId.longValue()));
                    IDS.remove(key);
                } catch (Exception e) {
                    throw new BackendException("");
                } finally {

                }
            }
        }

        private static byte[] b(long value) {
            return ByteBuffer.allocate(Long.BYTES).order(
                   ByteOrder.nativeOrder()).putLong(value).array();
        }

        private static long l(byte[] bytes) {
            assert bytes.length == Long.BYTES;
            return ByteBuffer.wrap(bytes).order(
                   ByteOrder.nativeOrder()).getLong();
        }

        public static void truncate(String graphName){
            String prefix = graphName + DELIMITER;
            for (Map.Entry<String, HgPair> entry:IDS.entrySet()) {
                if (entry.getKey().startsWith(prefix)){
                    IDS.remove(entry.getKey());
                }
            }
        }
    }

    public static class VertexLabel extends HstoreTable {

        public static final String TABLE = HugeType.VERTEX_LABEL.string();

        public VertexLabel(String database) {
            super(database, TABLE);
        }
    }

    public static class EdgeLabel extends HstoreTable {

        public static final String TABLE = HugeType.EDGE_LABEL.string();

        public EdgeLabel(String database) {
            super(database, TABLE);
        }
    }

    public static class PropertyKey extends HstoreTable {

        public static final String TABLE = HugeType.PROPERTY_KEY.string();

        public PropertyKey(String database) {
            super(database, TABLE);
        }
    }

    public static class IndexLabel extends HstoreTable {

        public static final String TABLE = HugeType.INDEX_LABEL.string();

        public IndexLabel(String database) {
            super(database, TABLE);
        }
    }

    public static class Vertex extends HstoreTable {

        public static final String TABLE = HugeType.VERTEX.string();

        public Vertex(String database) {
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
            super(database, (out ? 'o' : 'i') + TABLE_SUFFIX);
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

        public IndexTable(String database, String table) {
            super(database, table);
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
    }

    public static class SecondaryIndex extends IndexTable {

        public static final String TABLE = HugeType.SECONDARY_INDEX.string();

        public SecondaryIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class VertexLabelIndex extends IndexTable {

        public static final String TABLE =
                                   HugeType.VERTEX_LABEL_INDEX.string();

        public VertexLabelIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class EdgeLabelIndex extends IndexTable {

        public static final String TABLE = HugeType.EDGE_LABEL_INDEX.string();

        public EdgeLabelIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class SearchIndex extends IndexTable {

        public static final String TABLE = HugeType.SEARCH_INDEX.string();

        public SearchIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class UniqueIndex extends IndexTable {

        public static final String TABLE = HugeType.UNIQUE_INDEX.string();

        public UniqueIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class RangeIndex extends IndexTable {

        public RangeIndex(String database, String table) {
            super(database, table);
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
                begin = BinarySerializer.increaseOne(begin);
            }
            byte[] ownerStart = this.ownerScanDelegate.get();
            byte[] ownerEnd = this.ownerScanDelegate.get();
            if (max == null) {
                E.checkArgumentNotNull(prefix,
                                       "Range index prefix is missing");
                return session.scan(this.table(),ownerStart,ownerEnd,begin,
                                    prefix.asBytes(),Session.SCAN_PREFIX_END);
            } else {
                byte[] end = max.asBytes();
                int type = maxEq ? Session.SCAN_LTE_END : Session.SCAN_LT_END;
                return session.scan(this.table(), ownerStart,
                                    ownerEnd, begin, end, type);
            }
        }
    }

    public static class RangeIntIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_INT_INDEX.string();

        public RangeIntIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class RangeFloatIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_FLOAT_INDEX.string();

        public RangeFloatIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class RangeLongIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_LONG_INDEX.string();

        public RangeLongIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class RangeDoubleIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_DOUBLE_INDEX.string();

        public RangeDoubleIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class ShardIndex extends RangeIndex {

        public static final String TABLE = HugeType.SHARD_INDEX.string();

        public ShardIndex(String database) {
            super(database, TABLE);
        }
    }
    public static class OlapTable extends HstoreTable {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapTable(String database, Id id) {
            super(database, joinTableName(TABLE, id.asString()));
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }

        //@Override
        public boolean isOlap() {
            return true;
        }
    }

    public static class OlapSecondaryIndex extends SecondaryIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapSecondaryIndex(String store) {
            this(store, TABLE);
        }

        protected OlapSecondaryIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeIntIndex extends RangeIntIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapRangeIntIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeIntIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeLongIndex extends RangeLongIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapRangeLongIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeLongIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeFloatIndex extends RangeFloatIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapRangeFloatIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeFloatIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeDoubleIndex extends RangeDoubleIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapRangeDoubleIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeDoubleIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }
}
