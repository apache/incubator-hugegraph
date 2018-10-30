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

import com.baidu.hugegraph.type.HugeType;

public abstract class AbstractBackendStore implements BackendStore {

    private final MetaDispatcher dispatcher;

    public AbstractBackendStore() {
        this.dispatcher = new MetaDispatcher();
    }

    protected MetaDispatcher metaDispatcher() {
        return this.dispatcher;
    }

    public <Session extends BackendSession>
           void registerMetaHandler(String name, MetaHandler<Session> handler) {
        this.dispatcher.registerMetaHandler(name, handler);
    }

    // Get metadata by key
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        BackendSession session = this.session(type);
        MetaDispatcher dispatcher = null;
        if (type == null) {
            dispatcher = this.metaDispatcher();
        } else {
            BackendTable table = this.table(type);
            dispatcher = table.metaDispatcher();
        }
        return dispatcher.dispatchMetaHandler(session, meta, args);
    }

    protected abstract BackendTable table(HugeType type);

    // NOTE: Need to support passing null
    protected abstract BackendSession session(HugeType type);
}
