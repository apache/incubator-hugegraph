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

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;

public class CachedBackendStore implements BackendStore {

    private BackendStore store = null;
    private Cache cache = null;

    public CachedBackendStore(BackendStore store) {
        this.store = store;
        this.cache = CacheManager.instance().cache("store-" + name());
        // Set expire 30s
        this.cache.expire(30);
    }

    @Override
    public String name() {
        return this.store.name();
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
    public Object metadata(HugeType type, String meta, Object[] args) {
        return this.store.metadata(type, meta, args);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        // TODO: invalid cache, or set expire time at least
        this.store.mutate(mutation);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<BackendEntry> query(Query query) {
        if (query.empty()) {
            return this.store.query(query);
        }

        QueryId id = new QueryId(query);
        Object result = this.cache.get(id);
        if (result != null) {
            return (Iterable<BackendEntry>) result;
        } else {
            Iterable<BackendEntry> rs = this.store.query(query);
            if (rs.iterator().hasNext()) {
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
            // TODO Auto-generated method stub
            return hashCode() - o.hashCode();
        }

        @Override
        public Id prefixWith(HugeType type) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String asString() {
            return this.id.toString();
        }

        @Override
        public long asLong() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public byte[] asBytes() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String toString() {
            return this.id.toString();
        }
    }
}
