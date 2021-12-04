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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.util.Log;

public final class RocksDBIteratorPool implements AutoCloseable {

    private static final Logger LOG = Log.logger(RocksDBIteratorPool.class);

    private static final int ITERATOR_POOL_CAPACITY = 1;//CoreOptions.CPUS * 2;

    private final Queue<RocksIterator> pool;
    private final RocksDB rocksdb;
    private final ColumnFamilyHandle cfh;

    public RocksDBIteratorPool(RocksDB rocksdb, ColumnFamilyHandle cfh) {
        this.pool = new ArrayBlockingQueue<>(ITERATOR_POOL_CAPACITY);
        this.rocksdb = rocksdb;
        this.cfh = cfh;
    }

//    private RocksIterator allocIterator() {
//        return this.rocksdb.newIterator(this.cfh);
//    }
//
//    private void releaseIterator(RocksIterator iter) {
//        iter.close();
//    }

    public ReusedRocksIterator newIterator() {
        return new ReusedRocksIterator();
    }

    private RocksIterator allocIterator() {
        RocksIterator iter = this.pool.poll();
        if (iter != null && refreshIterator(iter)) {
            // Must refresh when an iterator is reused
            return iter;
        }
        /*
         * Create a new iterator if:
         *  - the pool is empty,
         *  - or the iterator obtained from the pool is closed,
         *  - or the iterator can't refresh.
         */
        iter = this.rocksdb.newIterator(this.cfh);
        LOG.debug("New iterator: {}", iter);
        try {
            iter.status();
            return iter;
        } catch (RocksDBException e) {
            iter.close();
            throw new BackendException(e);
        }
    }

    private void releaseIterator(RocksIterator iter) {
        assert iter.isOwningHandle();
        boolean added = this.pool.offer(iter);
        if (!added) {
            // Really close iterator if the pool is full
            LOG.debug("Really close iterator {} since the pool is full", iter);
            if (iter.isOwningHandle()) {
                iter.close();
            }
        } else {
            // Not sure whether it needs to refresh
            assert refreshIterator(iter);
        }
    }

    @Override
    public void close() {
        LOG.debug("Close IteratorPool with size {} ({})",
                  this.pool.size(), this);
        for (RocksIterator iter; (iter = this.pool.poll()) != null;) {
            if (iter.isOwningHandle()) {
                iter.close();
            }
        }
        assert this.pool.isEmpty();
    }

    private static boolean refreshIterator(RocksIterator iter) {
        if (iter.isOwningHandle()) {
            try {
                iter.refresh();
                return true;
            } catch (RocksDBException e) {
                LOG.warn("Can't refresh RocksIterator: {}", e.getMessage(), e);
            }
        }
        return false;
    }

    protected final class ReusedRocksIterator {

        private final RocksIterator iterator;
        private boolean closed;

        public ReusedRocksIterator() {
            this.iterator = allocIterator();
            this.closed = false;
        }

        public RocksIterator iterator() {
            assert !this.closed;
            return this.iterator;
        }

        public void close() {
            if (this.closed) {
                return;
            }
            this.closed = true;
            releaseIterator(this.iterator);
        }
    }
}
