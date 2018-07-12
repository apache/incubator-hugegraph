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

package com.baidu.hugegraph.backend.store;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;

public abstract class BackendTable<Session extends BackendSessionPool.Session,
                                   Entry> {

    private final String table;

    private final Map<String, MetaHandler<Session>> metaHandlers;

    public BackendTable(String table) {
        this.table = table;
        this.metaHandlers = new ConcurrentHashMap<>();

        this.registerMetaHandlers();
    }

    public String table() {
        return this.table;
    }

    @SuppressWarnings("unchecked")
    public <R> R metadata(Session session, String meta, Object... args) {
        if (!this.metaHandlers.containsKey(meta)) {
            throw new BackendException("Invalid metadata name '%s'", meta);
        }
        return (R) this.metaHandlers.get(meta).handle(session, meta, args);
    }

    public void registerMetaHandler(String name, MetaHandler<Session> handler) {
        this.metaHandlers.put(name, handler);
    }

    protected void registerMetaHandlers() {
        // pass
    }

    /**
     *  Mapping query-type to table-type
     * @param query origin query
     * @return corresponding table type
     */
    public static HugeType tableType(Query query) {
        HugeType type = query.resultType();

        // Mapping EDGE to EDGE_OUT/EDGE_IN
        if (type == HugeType.EDGE) {
            // We assume query OUT edges
            type = HugeType.EDGE_OUT;

            if (!query.ids().isEmpty() && query instanceof IdQuery &&
                query.originQuery() != null) {
                /*
                 * Some backends may trans ConditionQuery to IdQuery like
                 * RocksDB, so we should get the origin query
                 */
                query = query.originQuery();
            }

            if (!query.conditions().isEmpty() &&
                query instanceof ConditionQuery) {
                ConditionQuery cq = (ConditionQuery) query;
                // Does query IN edges
                if (cq.condition(HugeKeys.DIRECTION) == Directions.IN) {
                    type = HugeType.EDGE_IN;
                }
            }
        }

        return type;
    }

    public abstract void init(Session session);

    public abstract void clear(Session session);

    public abstract Iterator<BackendEntry> query(Session session, Query query);

    public abstract void insert(Session session, Entry entry);

    public abstract void delete(Session session, Entry entry);

    public abstract void append(Session session, Entry entry);

    public abstract void eliminate(Session session, Entry entry);

    public interface MetaHandler<Session extends BackendSessionPool.Session> {
        public Object handle(Session session, String meta, Object... args);
    }
}
