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

package com.baidu.hugegraph.backend.cache;

import java.util.Iterator;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;

/**
 * This class is unused now, just for debug or test
 */
public class CachedBackendStore implements BackendStore {

    private BackendStore store = null;
    private Cache cache = null;

    public CachedBackendStore(BackendStore store) {
        this.store = store;
        this.cache = CacheManager.instance().cache("store-" + store());
        // Set expire 30s
        this.cache.expire(30);
    }

    @Override
    public String store() {
        return this.store.store();
    }

    @Override
    public String database() {
        return this.store.database();
    }

    @Override
    public BackendStoreProvider provider() {
        return this.store.provider();
    }

    @Override
    public void open(HugeConfig config) {
        this.store.open(config);
    }

    @Override
    public void close() {
        this.store.close();
    }

    @Override
    public void init() {
        this.store.init();
    }

    @Override
    public void clear() {
        this.store.clear();
    }

    @Override
    public void beginTx() {
        this.store.beginTx();
    }

    @Override
    public void commitTx() {
        this.store.commitTx();
    }

    @Override
    public void rollbackTx() {
        this.store.rollbackTx();
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        return this.store.metadata(type, meta, args);
    }

    @Override
    public BackendFeatures features() {
        return this.store.features();
    }

    @Override
    public Id nextId(HugeType type) {
        return this.store.nextId(type);
    }

    @Override
    public void increaseCounter(HugeType type, long increment) {
        this.store.increaseCounter(type, increment);
    }

    @Override
    public long getCounter(HugeType type) {
        return this.store.getCounter(type);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        // TODO: invalid cache, or set expire time at least
        this.store.mutate(mutation);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<BackendEntry> query(Query query) {
        if (query.empty()) {
            return this.store.query(query);
        }

        QueryId id = new QueryId(query);
        Object result = this.cache.get(id);
        if (result != null) {
            return (Iterator<BackendEntry>) result;
        } else {
            Iterator<BackendEntry> rs = this.store.query(query);
            if (rs.hasNext()) {
                this.cache.update(id, rs);
            }
            return rs;
        }
    }

    /**
     * Query as an Id for cache
     */
    static class QueryId implements Id {

        private Query id;
        private int hashCode;

        public QueryId(Query q) {
            this.id = q;
            this.hashCode = this.id.hashCode();
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof QueryId)) {
                return false;
            }
            return this.id.equals(((QueryId) other).id);
        }

        @Override
        public int compareTo(Id o) {
            // TODO: improve
            return this.hashCode() - o.hashCode();
        }

        @Override
        public Object asObject() {
            // TODO: improve
            return null;
        }

        @Override
        public String asString() {
            return this.id.toString();
        }

        @Override
        public long asLong() {
            // TODO: improve
            return 0;
        }

        @Override
        public byte[] asBytes() {
            // TODO: improve
            return null;
        }

        @Override
        public String toString() {
            return this.id.toString();
        }

        @Override
        public int length() {
            // TODO: improve
            return 32;
        }

        @Override
        public boolean number() {
            // TODO: improve
            return false;
        }
    }
}
