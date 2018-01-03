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

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.store.BackendStore.TxState;
import com.baidu.hugegraph.util.Log;

public abstract class BackendSessionPool {

    private static final Logger LOG = Log.logger(BackendSessionPool.class);

    private ThreadLocal<Session> threadLocalSession;
    private AtomicInteger sessionCount;

    public BackendSessionPool() {
        this.threadLocalSession = new ThreadLocal<>();
        this.sessionCount = new AtomicInteger(0);
    }

    public final Session getOrNewSession() {
        Session session = this.threadLocalSession.get();
        if (session == null) {
            session = this.newSession();
            assert session != null;
            this.threadLocalSession.set(session);
            this.sessionCount.incrementAndGet();
            LOG.debug("Now(after connect({})) session count is: {}",
                      this, this.sessionCount.get());
        }
        return session;
    }

    public Session useSession() {
        Session session = this.threadLocalSession.get();
        if (session != null) {
            session.attach();
        } else {
            session = this.getOrNewSession();
        }
        return session;
    }

    public int closeSession() {
        Session session = this.threadLocalSession.get();
        if (session == null) {
            LOG.warn("Current session has ever been closed");
            return -1;
        }
        int ref = session.detach();
        assert ref >= 0 : ref;
        if (ref == 0) {
            try {
                session.close();
            } catch (Throwable e) {
                session.attach();
                throw e;
            }
            this.threadLocalSession.remove();
            this.sessionCount.decrementAndGet();
        }
        return ref;
    }

    public void close() {
        int ref = -1;
        try {
            ref = this.closeSession();
        } finally {
            if (this.sessionCount.get() == 0) {
                this.doClose();
            }
        }
        LOG.debug("Now(after close({})) session count is: {}, " +
                  "current session reference is: {}",
                  this, this.sessionCount.get(), ref);
    }

    public boolean closed() {
        return this.sessionCount.get() == 0;
    }

    @Override
    public String toString() {
        return String.format("%s@%08X",
                             this.getClass().getSimpleName(),
                             this.hashCode());
    }

    protected abstract Session newSession();

    protected abstract void doClose();

    /**
     * interface Session for backend store
     */
    public static abstract class Session {

        private int refs;
        private TxState txState;

        public Session() {
            this.refs = 1;
            this.txState = TxState.CLEAN;
        }

        public abstract void close();

        public abstract boolean closed();

        public abstract void clear();

        public abstract Object commit();

        public abstract boolean hasChanges();

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
}
