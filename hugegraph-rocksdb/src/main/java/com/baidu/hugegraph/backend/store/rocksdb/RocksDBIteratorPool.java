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

//    public RocksIterator allocIterator() {
//        return this.rocksdb.newIterator(this.cfh);
//    }
//
//    public void releaseIterator(RocksIterator iter) {
//        iter.close();
//    }

    public RocksIterator allocIterator() {
        RocksIterator iter = this.pool.poll();
        if (iter != null && iter.isOwningHandle()) {
            try {
                iter.refresh();
                return iter;
            } catch (RocksDBException e) {
                LOG.warn("Can't refresh RocksIterator: {}", e.getMessage(), e);
            }
        }
        /*
         * Create a new iterator if:
         *  - the pool is empty,
         *  - or the iterator obtained from the pool is closed,
         *  - or the iterator can't refresh.
         */
        iter = this.rocksdb.newIterator(this.cfh);
        LOG.debug("New iterator: {}", iter);
        return iter;
    }

    public void releaseIterator(RocksIterator iter) {
        assert iter.isOwningHandle();
        boolean added = this.pool.offer(iter);
        if (!added) {
            // Really close iterator if the pool is full
            LOG.debug("Really close iterator {} since the pool is full", iter);
            if (iter.isOwningHandle()) {
                iter.close();
            }
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

    protected static final class ReusedRocksIterator {

        private final RocksIterator iterator;
        private final RocksDBIteratorPool iterPool;
        private boolean closed;

        public ReusedRocksIterator(RocksDBIteratorPool iterPool) {
            this.iterPool = iterPool;
            this.iterator = iterPool.allocIterator();
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
            this.iterPool.releaseIterator(this.iterator);
        }
    }
}
