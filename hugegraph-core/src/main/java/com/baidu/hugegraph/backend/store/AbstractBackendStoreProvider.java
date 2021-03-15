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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.Log;

public abstract class AbstractBackendStoreProvider
                implements BackendStoreProvider {

    private static final Logger LOG = Log.logger(BackendStoreProvider.class);

    private String graph = null;

    private EventHub storeEventHub = new EventHub("store");

    protected Map<String, BackendStore> stores = null;

    protected final void notifyAndWaitEvent(String event) {
        Future<?> future = this.storeEventHub.notify(event, this);
        try {
            future.get();
        } catch (Throwable e) {
            LOG.warn("Error when waiting for event execution: {}", event, e);
        }
    }

    protected final void checkOpened() {
        E.checkState(this.graph != null && this.stores != null,
                     "The BackendStoreProvider has not been opened");
    }

    protected abstract BackendStore newSchemaStore(String store);

    protected abstract BackendStore newGraphStore(String store);

    @Override
    public void listen(EventListener listener) {
        this.storeEventHub.listen(EventHub.ANY_EVENT, listener);
    }

    @Override
    public void unlisten(EventListener listener) {
        this.storeEventHub.unlisten(EventHub.ANY_EVENT, listener);
    }

    @Override
    public String graph() {
        this.checkOpened();
        return this.graph;
    }

    @Override
    public void open(String graph) {
        LOG.debug("Graph '{}' open StoreProvider", this.graph);
        E.checkArgument(graph != null, "The graph name can't be null");
        E.checkArgument(!graph.isEmpty(), "The graph name can't be empty");

        this.graph = graph;
        this.stores = new ConcurrentHashMap<>();

        this.storeEventHub.notify(Events.STORE_OPEN, this);
    }

    @Override
    public void waitStoreStarted() {
        // pass
    }

    @Override
    public void close() throws BackendException {
        LOG.debug("Graph '{}' close StoreProvider", this.graph);
        this.checkOpened();
        this.storeEventHub.notify(Events.STORE_CLOSE, this);
    }

    @Override
    public void init() {
        this.checkOpened();
        for (BackendStore store : this.stores.values()) {
            store.init();
        }
        this.notifyAndWaitEvent(Events.STORE_INIT);

        LOG.debug("Graph '{}' store has been initialized", this.graph);
    }

    @Override
    public void clear() throws BackendException {
        this.checkOpened();
        for (BackendStore store : this.stores.values()) {
            // Just clear tables of store, not clear space
            store.clear(false);
        }
        for (BackendStore store : this.stores.values()) {
            // Only clear space of store
            store.clear(true);
        }
        this.notifyAndWaitEvent(Events.STORE_CLEAR);

        LOG.debug("Graph '{}' store has been cleared", this.graph);
    }

    @Override
    public void truncate() {
        this.checkOpened();
        for (BackendStore store : this.stores.values()) {
            store.truncate();
        }
        this.notifyAndWaitEvent(Events.STORE_TRUNCATE);

        LOG.debug("Graph '{}' store has been truncated", this.graph);
    }

    @Override
    public void initSystemInfo(HugeGraph graph) {
        this.checkOpened();
        BackendStoreSystemInfo info = graph.backendStoreSystemInfo();
        info.init();
        this.notifyAndWaitEvent(Events.STORE_INITED);

        LOG.debug("Graph '{}' system info has been initialized", this.graph);
    }

    @Override
    public void writeSnapshot() {
        String snapshotPrefix = "snapshot";
        for (BackendStore store : this.stores.values()) {
            store.writeSnapshot(snapshotPrefix);
        }
    }

    @Override
    public void readSnapshot() {
        throw new UnsupportedOperationException("readSnapshot");
    }

    @Override
    public BackendStore loadSchemaStore(final String name) {
        LOG.debug("The '{}' StoreProvider load SchemaStore '{}'",
                  this.type(), name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = this.newSchemaStore(name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        LOG.debug("The '{}' StoreProvider load GraphStore '{}'",
                  this.type(),  name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = this.newGraphStore(name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        return store;
    }

    @Override
    public BackendStore loadSystemStore(String name) {
        return this.loadGraphStore(name);
    }

    @Override
    public EventHub storeEventHub() {
        return this.storeEventHub;
    }
}
