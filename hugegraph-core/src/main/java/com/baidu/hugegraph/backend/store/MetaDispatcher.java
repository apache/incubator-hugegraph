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

import com.baidu.hugegraph.exception.NotSupportException;

public class MetaDispatcher<Session extends BackendSession> {

    protected final Map<String, MetaHandler<Session>> metaHandlers;

    public MetaDispatcher() {
        this.metaHandlers = new ConcurrentHashMap<>();
    }

    public void registerMetaHandler(String meta, MetaHandler<Session> handler) {
        this.metaHandlers.put(meta, handler);
    }

    @SuppressWarnings("unchecked")
    public <R> R dispatchMetaHandler(Session session,
                                     String meta, Object[] args) {
        if (!this.metaHandlers.containsKey(meta)) {
            throw new NotSupportException("metadata '%s'", meta);
        }
        return (R) this.metaHandlers.get(meta).handle(session, meta, args);
    }
}
