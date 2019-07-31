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

package com.baidu.hugegraph.backend.store.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.memory.InMemoryDBStore.InMemoryGraphStore;
import com.baidu.hugegraph.backend.store.memory.InMemoryDBStore.InMemorySchemaStore;
import com.baidu.hugegraph.util.Events;

public class InMemoryDBStoreProvider extends AbstractBackendStoreProvider {

    public static final String TYPE = "memory";

    private static Map<String, InMemoryDBStoreProvider> providers = null;

    public static boolean matchType(String type) {
        return TYPE.equalsIgnoreCase(type);
    }

    public static synchronized InMemoryDBStoreProvider instance(String graph) {
        if (providers == null) {
            providers = new ConcurrentHashMap<>();
        }
        if (!providers.containsKey(graph)) {
            InMemoryDBStoreProvider p = new InMemoryDBStoreProvider(graph);
            providers.putIfAbsent(graph, p);
        }
        return providers.get(graph);
    }

    private InMemoryDBStoreProvider(String graph) {
        this.open(graph);
    }

    @Override
    public void open(String graph) {
        super.open(graph);
        /*
         * Memory store need to init some system property,
         * like task related property-keys and vertex-labels.
         * don't notify from store.open() due to task-tx will
         * call it again and cause dead
         */
        this.notifyAndWaitEvent(Events.STORE_INIT);
    }

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new InMemorySchemaStore(this, this.graph(), store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new InMemoryGraphStore(this, this.graph(), store);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String version() {
        /*
         * Versions history:
         * [1.0] HugeGraph-1328: supports backend table version checking
         * [1.1] HugeGraph-1322: add support for full-text search
         * [1.2] #296: support range sortKey feature
         * [1.3] #270 & #398: support shard-index and vertex + sortkey prefix
         */
        return "1.3";
    }
}
