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

import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.type.HugeType;

public abstract class AbstractBackendStore<Session extends BackendSession>
                implements BackendStore {

    private final SystemSchemaStore systemSchemaStore;
    private final MetaDispatcher<Session> dispatcher;

    public AbstractBackendStore() {
        this.systemSchemaStore = new SystemSchemaStore();
        this.dispatcher = new MetaDispatcher<>();
    }

    protected MetaDispatcher<Session> metaDispatcher() {
        return this.dispatcher;
    }

    public void registerMetaHandler(String name, MetaHandler<Session> handler) {
        this.dispatcher.registerMetaHandler(name, handler);
    }

    @Override
    public String storedVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemSchemaStore systemSchemaStore() {
        return this.systemSchemaStore;
    }

    // Get metadata by key
    @Override
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        Session session = this.session(type);
        MetaDispatcher<Session> dispatcher;
        if (type == null) {
            dispatcher = this.metaDispatcher();
        } else {
            BackendTable<Session, ?> table = this.table(type);
            dispatcher = table.metaDispatcher();
        }
        return dispatcher.dispatchMetaHandler(session, meta, args);
    }

    protected void checkOpened() throws ConnectionException {
        if (!this.opened()) {
            throw new ConnectionException(
                      "The '%s' store of %s has not been opened",
                      this.database(), this.provider().type());
        }
    }

    @Override
    public String toString() {
        return String.format("%s/%s", this.database(), this.store());
    }

    protected abstract BackendTable<Session, ?> table(HugeType type);

    // NOTE: Need to support passing null
    protected abstract Session session(HugeType type);
}
