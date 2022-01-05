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

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.HgKvIterator;
import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgSessionManager;
import com.baidu.hugegraph.store.HgStoreSession;
import com.baidu.hugegraph.store.client.HgStoreNodeManager;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HstoreSessionsImpl extends HstoreSessions {

    private final HugeConfig config;
    private final HstoreSession session;
    private final Map<String, Integer> tables;
    private final AtomicInteger refCount;
    private static int tableCode = 0;
    private final String graphName;
    private final String database;
    private static volatile Boolean INITIALIZED_NODE = Boolean.FALSE;
    private static volatile PDClient defaultPdClient;
    private static volatile HstoreNodePartitionerImpl nodePartitioner = null;
    private static volatile Set<String> INFO_INITIALIZED_GRAPH =
                            Collections.synchronizedSet(new HashSet<>());
    public HstoreSessionsImpl(HugeConfig config, String database,
                              String store) {
        super(config, database, store);
        this.config = config;
        this.database = database;
        this.graphName = database + "/" + store;
        initStoreNode(config);
        this.session = new HstoreSession(this.config, graphName);
        this.tables = new ConcurrentHashMap<>();
        this.refCount = new AtomicInteger(1);
    }

    private void initStoreNode(HugeConfig config) {
        if (!INITIALIZED_NODE) {
            synchronized (INITIALIZED_NODE) {
                if (!INITIALIZED_NODE) {
                    HgStoreNodeManager nodeManager =
                                       HgStoreNodeManager.getInstance();
                    defaultPdClient = PDClient.create(PDConfig.of(
                                      config.get(HstoreOptions.PD_PEERS)));
                    nodePartitioner = FakeHstoreNodePartitionerImpl
                                      .NodePartitionerFactory
                                      .getNodePartitioner(config,nodeManager);
                    nodeManager.setNodeProvider(nodePartitioner);
                    nodeManager.setNodePartitioner(nodePartitioner);
                    nodeManager.setNodeNotifier(nodePartitioner);
                    INITIALIZED_NODE = Boolean.TRUE;
                }
            }
        }
    }
    public static HstoreNodePartitionerImpl getNodePartitioner() {
        return nodePartitioner;
    }
    public static PDClient getDefaultPdClient() {
        return defaultPdClient;
    }

    @Override
    public void open() throws Exception {
        if (!INFO_INITIALIZED_GRAPH.contains(this.graphName)) {
            synchronized (INITIALIZED_NODE){
                if (!INFO_INITIALIZED_GRAPH.contains(this.graphName)) {
                    Integer partitionCount =
                            this.config.get(HstoreOptions.PARTITION_COUNT);
                    Assert.assertTrue("The value of hstore.partition_count" +
                                      " cannot be less than 0.",
                                      partitionCount > -1);
                    Integer shareCount = this.config.get(HstoreOptions.SHARD_COUNT);
                    Assert.assertTrue("The value of hstore.shard_count" +
                                      " cannot be less than 0.",
                                      shareCount > -1);
                    defaultPdClient.setGraph(Metapb.Graph.newBuilder()
                                   .setGraphName(this.graphName)
                                   .setPartitionCount(partitionCount)
                                   .setShardCount(shareCount).build());
                    INFO_INITIALIZED_GRAPH.add(this.graphName);
                }
            }
        }
        this.session.open();
    }

    @Override
    protected boolean opened() {
        return this.session != null;
    }

    @Override
    public Set<String> openedTables() {
        return this.tables.keySet();
    }

    @Override
    public synchronized void createTable(String... tables) {
        for (String table : tables) {
            this.tables.put(table, tableCode++);
        }
    }

    @Override
    public synchronized void dropTable(String... tables) {
        for (String table : tables) {
            this.tables.remove(table);
        }
    }

    @Override
    public boolean existsTable(String table) {
        return this.tables.containsKey(table);
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected final Session newSession() {
        return new HstoreSession(this.config(), this.graphName);
    }

    @Override
    protected synchronized void doClose() {
        this.checkValid();

        if (this.refCount.decrementAndGet() > 0) {
            return;
        }
        assert this.refCount.get() == 0;
        this.tables.clear();
        this.session.close();
    }

    private void checkValid() {
    }

    public static byte[] encode(String string) {
        return StringEncoding.encode(string);
    }

    public static String decode(byte[] bytes) {
        return StringEncoding.decode(bytes);
    }


    /**
     * HstoreSession implement for hstore
     */
    private final class HstoreSession extends Session {

        private Map<String, Map<HgOwnerKey, byte[]>> putBatch;
        private Map<String, Set<HgOwnerKey>> deleteBatch;
        private Map<String, List<HgOwnerKey>> deletePrefixBatch;
        private Map<String, MutablePair<HgOwnerKey,
                                        HgOwnerKey>> deleteRangeBatch;
        private HgStoreSession graph;

        public HstoreSession(HugeConfig conf, String graphName) {
            resetBuffer();
            setGraphName(graphName);
            setConf(conf);
            HgSessionManager manager = HgSessionManager.getInstance();
            this.graph = manager.openSession(graphName);
        }

        @Override
        public void open() {
            this.opened = true;
        }

        @Override
        public void close() {
            this.opened = false;
        }

        @Override
        public boolean closed() {
            return !this.opened;
        }

        @Override
        public void reset() {
            resetBuffer();
        }

        private void resetBuffer() {
            this.putBatch = new HashMap<String, Map<HgOwnerKey, byte[]>>();
            this.deleteBatch = new HashMap<String, Set<HgOwnerKey>>();
            this.deletePrefixBatch = new HashMap<String, List<HgOwnerKey>>();
            this.deleteRangeBatch = new HashMap<String,
                                    MutablePair<HgOwnerKey, HgOwnerKey>>();
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return this.size() > 0;
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {

            int count = this.size();
            if (count <= 0) {
                return 0;
            }

            if (this.putBatch.size() > 0) {
                this.graph.batchPutOwner(this.putBatch);
                this.putBatch.clear();
            }

            if (this.deleteBatch.size() > 0) {
                this.graph.batchDeleteOwner(this.deleteBatch);
                this.deletePrefixBatch.clear();
            }

            if (this.deletePrefixBatch.size() > 0) {
                this.deletePrefixBatch.forEach((table,keys)->{
                    keys.forEach(key->{
                        this.graph.deletePrefix(table,key);
                    });
                });
                this.deletePrefixBatch.clear();
            }

            if (this.deleteRangeBatch.size() > 0) {
                this.deleteRangeBatch.forEach((table,keys)->{
                    this.graph.deleteRange(table,keys.getLeft(),
                                           keys.getRight());
                });
                this.deleteRangeBatch.clear();
            }

            return count;
        }

        /**
         * Rollback all updates(put/delete) not committed
         */
        @Override
        public void rollback() {
            this.putBatch.clear();
            this.deleteBatch.clear();
            this.deletePrefixBatch.clear();
            this.deleteRangeBatch.clear();
        }

        @Override
        public Pair<byte[], byte[]> keyRange(String table) {
            // TODO: get first key and lastkey of fake tikv table
            return null;
        }

        /**
         * Add a KV record to a table
         */
        @Override
        public void put(String table, byte[] ownerKey, byte[] key, byte[] value) {
            Map<HgOwnerKey, byte[]> valueMap = this.putBatch.get(table);
            if (valueMap == null) {
                valueMap = new HashMap<HgOwnerKey, byte[]>();
                this.putBatch.put(table, valueMap);
            }
            valueMap.put(HgOwnerKey.of(ownerKey, key), value);

        }

        @Override
        public synchronized void increase(String table, byte[] ownerKey,
                                          byte[] key, byte[] value) {
            this.graph.merge(table, HgOwnerKey.of(ownerKey, key), value);
        }

        @Override
        public void delete(String table, byte[] ownerKey, byte[] key) {
            Map valueMap = this.putBatch.get(table);
            if (valueMap != null && valueMap.remove(key) != null) {
                return;
            }
            Set<HgOwnerKey> valueSet = this.deleteBatch.get(table);
            if (valueSet == null) {
                valueSet = new HashSet<HgOwnerKey>();
                this.deleteBatch.put(table, valueSet);
            }
            valueSet.add(HgOwnerKey.of(ownerKey, key));
        }

        @Override
        public void deletePrefix(String table,byte[] ownerKey, byte[] key) {
            List<HgOwnerKey> values = this.deletePrefixBatch.get(table);
            if (values == null) {
                values = new ArrayList<HgOwnerKey>();
                this.deletePrefixBatch.put(table, values);
            }
            values.add(HgOwnerKey.of(ownerKey, key));
        }

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void deleteRange(String table,byte[] ownerKeyFrom,
                                byte[] ownerKeyTo, byte[] keyFrom,
                                byte[] keyTo) {
            this.deleteRangeBatch.put(table,
                                      new MutablePair<HgOwnerKey, HgOwnerKey>(
                                          HgOwnerKey.of(ownerKeyFrom, keyFrom),
                                          HgOwnerKey.of(ownerKeyTo, keyTo)));
        }

        @Override
        public byte[] get(String table, byte[] key) {
            return this.graph.get(table,
                   HgOwnerKey.of(HgStoreClientConst.ALL_PARTITION_OWNER, key));
        }

        @Override
        public byte[] get(String table, byte[] ownerKey, byte[] key) {
            byte[] values = this.graph.get(table,
                                           HgOwnerKey.of(ownerKey, key));
            return values != null ? values : new byte[0];
        }

        @Override
        public BackendColumnIterator scan(String table) {
            assert !this.hasChanges();
            HgKvIterator results =  this.graph.scanIterator(table);
            return new ColumnIterator<HgKvIterator>(table, results);
        }

        @Override
        public BackendColumnIterator scan(String table,
                                          byte[] conditionQueryToByte) {
            assert !this.hasChanges();
            HgKvIterator results =
                         this.graph.scanIterator(table, conditionQueryToByte);
            return new ColumnIterator<HgKvIterator>(table, results);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKey,
                                          byte[] prefix) {
            assert !this.hasChanges();
            HgKvIterator result = this.graph.scanIterator(table,
                                  HgOwnerKey.of(ownerKey, prefix));
            return new ColumnIterator<HgKvIterator>(table, result);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo, byte[] keyFrom,
                                          byte[] keyTo, int scanType) {
            assert !this.hasChanges();
            HgKvIterator result = this.graph.scanIterator(table,
                                  HgOwnerKey.of(ownerKeyFrom,keyFrom),
                                  HgOwnerKey.of(ownerKeyTo,keyTo),
                                  scanType);
            return new ColumnIterator<HgKvIterator>(table, result, keyFrom,
                                                    keyTo, scanType);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo, byte[] keyFrom,
                                          byte[] keyTo, int scanType,
                                          byte[] query) {
            assert !this.hasChanges();
            HgKvIterator result = this.graph.scanIterator(table,
                                  HgOwnerKey.of(ownerKeyFrom,keyFrom),
                                  HgOwnerKey.of(ownerKeyTo,keyTo),
                                  scanType,query);
            return new ColumnIterator<HgKvIterator>(table, result, keyFrom,
                                                    keyTo, scanType);
        }

        @Override
        public void merge(String table, byte[] ownerKey,
                          byte[] key, byte[] value) {
            this.graph.merge(table,HgOwnerKey.of(ownerKey,key),value);
        }

        @Override
        public void setMode(GraphMode mode) {
            /*
            HgStoreNodePartitioner partitioner =
                                   HgStoreNodeManager.getInstance()
                                                     .getNodePartitioner();
            if (partitioner instanceof HgStoreNodePartitioner) {
                ((HstoreNodePartitionerImpl) partitioner).setWorkMode(this.getGraphName(), mode.equals(GraphMode.LOADING) ?
                        Metapb.GraphWorkMode.Batch_Import :
                        Metapb.GraphWorkMode.Normal);
            }
            */
        }

        @Override
        public void truncate() throws Exception {
            this.graph.truncate();
            HstoreSessionsImpl.getDefaultPdClient()
                              .resetIdByKey(this.getGraphName());
            HstoreTables.Counters.truncate(this.getGraphName());
        }

        private int size() {
            return this.putBatch.size() +
                   this.deleteBatch.size() +
                   this.deletePrefixBatch.size() +
                   this.deleteRangeBatch.size();
        }
    }

    private static class ColumnIterator<T extends HgKvIterator> implements BackendColumnIterator, Countable {

        private final String table;
        private final T iter;

        private final byte[] keyBegin;
        private final byte[] keyEnd;
        private final int scanType;

        private byte[] position;
        private byte[] value;
        private boolean matched;

        public ColumnIterator(String table, T results) {
            this(table, results, null, null, 0);
        }

        public T iter() {
            return iter;
        }

        public ColumnIterator(String table, T results, byte[] keyBegin,
                              byte[] keyEnd, int scanType) {
            E.checkNotNull(results, "results");
            this.table = table;
            this.iter = results;
            this.keyBegin = keyBegin;
            this.keyEnd = keyEnd;
            this.scanType = scanType;
            this.position = keyBegin;
            this.value = null;
            this.matched = false;

            this.checkArguments();
        }

        private void checkArguments() {
            E.checkArgument(!(this.match(Session.SCAN_PREFIX_BEGIN) &&
                            this.match(Session.SCAN_PREFIX_END)),
                            "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                            "SCAN_PREFIX_WITH_END at the same time");

            E.checkArgument(!(this.match(Session.SCAN_PREFIX_BEGIN) &&
                            this.match(Session.SCAN_GT_BEGIN)),
                            "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                            "SCAN_GT_BEGIN/SCAN_GTE_BEGIN at the same time");

            E.checkArgument(!(this.match(Session.SCAN_PREFIX_END) &&
                            this.match(Session.SCAN_LT_END)),
                            "Can't set SCAN_PREFIX_WITH_END and " +
                            "SCAN_LT_END/SCAN_LTE_END at the same time");

            if (this.match(Session.SCAN_PREFIX_BEGIN)) {
                E.checkArgument(this.keyBegin != null,
                                "Parameter `keyBegin` can't be null " +
                                "if set SCAN_PREFIX_WITH_BEGIN");
                E.checkArgument(this.keyEnd == null,
                                "Parameter `keyEnd` must be null " +
                                "if set SCAN_PREFIX_WITH_BEGIN");
            }

            if (this.match(Session.SCAN_PREFIX_END)) {
                E.checkArgument(this.keyEnd != null,
                                "Parameter `keyEnd` can't be null " +
                                "if set SCAN_PREFIX_WITH_END");
            }

            if (this.match(Session.SCAN_GT_BEGIN)) {
                E.checkArgument(this.keyBegin != null,
                                "Parameter `keyBegin` can't be null " +
                                "if set SCAN_GT_BEGIN or SCAN_GTE_BEGIN");
            }

            if (this.match(Session.SCAN_LT_END)) {
                E.checkArgument(this.keyEnd != null,
                                "Parameter `keyEnd` can't be null " +
                                "if set SCAN_LT_END or SCAN_LTE_END");
            }
        }

        private boolean match(int expected) {
            return Session.matchScanType(expected, this.scanType);
        }

        @Override
        public boolean hasNext() {
            this.matched = false;
            return this.iter.hasNext();
        }

        private boolean filter(byte[] key) {
            if (this.match(Session.SCAN_PREFIX_BEGIN)) {
                /*
                 * Prefix with `keyBegin`?
                 * TODO: use custom prefix_extractor instead
                 *       or use ReadOptions.prefix_same_as_start
                 */
                return Bytes.prefixWith(key, this.keyBegin);
            } else if (this.match(Session.SCAN_PREFIX_END)) {
                /*
                 * Prefix with `keyEnd`?
                 * like the following query for range index:
                 *  key > 'age:20' and prefix with 'age'
                 */
                assert this.keyEnd != null;
                return Bytes.prefixWith(key, this.keyEnd);
            } else if (this.match(Session.SCAN_LT_END)) {
                /*
                 * Less (equal) than `keyEnd`?
                 * NOTE: don't use BytewiseComparator due to signed byte
                 */
                assert this.keyEnd != null;
                if (this.match(Session.SCAN_LTE_END)) {
                    // Just compare the prefix, can be there are excess tail
                    key = Arrays.copyOfRange(key, 0, this.keyEnd.length);
                    return Bytes.compare(key, this.keyEnd) <= 0;
                } else {
                    return Bytes.compare(key, this.keyEnd) < 0;
                }
            } else {
                assert this.match(Session.SCAN_ANY) ||
                       this.match(Session.SCAN_GT_BEGIN) ||
                       this.match(Session.SCAN_GTE_BEGIN) : "Unknow scan type";
                return true;
            }
        }

        @Override
        public BackendEntry.BackendColumn next() {
            if (!this.matched && !this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.iter.next();
            BackendEntry.BackendColumn col = BackendEntry.BackendColumn.of(
                                       this.iter.key(), this.iter.value());
            this.matched = false;
            return col;
        }

        @Override
        public long count() {
            long count = 0L;
            while (this.hasNext()) {
                this.next();
                count++;
                this.matched = false;
                BackendEntryIterator.checkInterrupted();
            }
            return count;
        }

        @Override
        public byte[] position() {
            return this.position;
        }

        @Override
        public void close() {}
    }
}
