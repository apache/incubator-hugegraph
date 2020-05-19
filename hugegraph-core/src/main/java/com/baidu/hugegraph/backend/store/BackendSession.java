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
public interface BackendSession {

    public void open();
    public void close();

    public boolean opened();
    public boolean closed();

    public Object commit();

    public void rollback();

    public boolean hasChanges();

    public int attach();
    public int detach();

    public long created();
    public long updated();
    public void update();

    public default void reconnectIfNeeded() {
        // pass
    }

    public default void reset() {
        // pass
    }

    public abstract class AbstractBackendSession implements BackendSession {

        protected boolean opened;
        private int refs;
        private TxState txState;
        private final long created;
        private long updated;

        public AbstractBackendSession() {
            this.opened = true;
            this.refs = 1;
            this.txState = TxState.CLEAN;
            this.created = System.currentTimeMillis();
            this.updated = this.created;
        }

        @Override
        public long created() {
            return this.created;
        }

        @Override
        public long updated() {
            return this.updated;
        }

        @Override
        public void update() {
            this.updated = System.currentTimeMillis();
        }

        @Override
        public boolean opened() {
            return this.opened;
        }

        @Override
        public boolean closed() {
            return !this.opened;
        }

        @Override
        public int attach() {
            return ++this.refs;
        }

        @Override
        public int detach() {
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
}
