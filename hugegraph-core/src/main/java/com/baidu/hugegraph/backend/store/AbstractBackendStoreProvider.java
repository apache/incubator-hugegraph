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

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;

public abstract class AbstractBackendStoreProvider
                implements BackendStoreProvider {

    protected String name;
    protected Map<String, BackendStore> stores;

    private EventHub storeEventHub = new EventHub("store");

    @Override
    public void listen(EventListener listener) {
        this.storeEventHub.listen(EventHub.ANY_EVENT, listener);
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void open(String name) {
        E.checkNotNull(name, "store name");

        this.name = name;
        this.stores = new ConcurrentHashMap<>();

        this.storeEventHub.notify(Events.STORE_OPEN, this);
    }

    @Override
    public void close() throws BackendException {
        for (BackendStore store : this.stores.values()) {
            // TODO: catch exceptions here
            store.close();
        }
        this.storeEventHub.notify(Events.STORE_CLOSE, this);
    }

    @Override
    public void init() {
        for (BackendStore store : this.stores.values()) {
            store.init();
        }
        this.storeEventHub.notify(Events.STORE_INIT, this);
    }

    @Override
    public void clear() throws BackendException {
        for (BackendStore store : this.stores.values()) {
            store.clear();
        }
        this.storeEventHub.notify(Events.STORE_CLEAR, this);
    }
}
