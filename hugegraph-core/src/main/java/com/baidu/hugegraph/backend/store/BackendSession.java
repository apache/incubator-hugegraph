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

import com.baidu.hugegraph.backend.store.BackendStore.TxState;

/**
 * interface Session for backend store
 */
public abstract class BackendSession {

    protected boolean opened;
    private int refs;
    private TxState txState;
    private final long created;
    private long updated;

    public BackendSession() {
        this.opened = true;
        this.refs = 1;
        this.txState = TxState.CLEAN;
        this.created = System.currentTimeMillis();
        this.updated = this.created;
    }

    public long created() {
        return this.created;
    }

    public long updated() {
        return this.updated;
    }

    public void update() {
        this.updated = System.currentTimeMillis();
    }

    public abstract void open();
    public abstract void close();

    public abstract Object commit();

    public abstract void rollback();

    public abstract boolean hasChanges();

    protected void reconnectIfNeeded() {
        // pass
    }

    protected void reset() {
        // pass
    }

    public boolean opened() {
        return this.opened;
    }

    public boolean closed() {
        return !this.opened;
    }

    protected int attach() {
        return ++this.refs;
    }

    protected int detach() {
        return --this.refs;
    }

    public boolean closeable() {
        return this.refs <= 0;
    }

    public TxState txState() {
        return this.txState;
    }

    public void txState(TxState state) {
        this.txState = state;
    }
}
