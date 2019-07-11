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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public abstract class BackendSessionPool {

    private static final Logger LOG = Log.logger(BackendSessionPool.class);

    private final HugeConfig config;
    private final String name;
    private final ThreadLocal<BackendSession> threadLocalSession;
    private final AtomicInteger sessionCount;

    public BackendSessionPool(HugeConfig config, String name) {
        this.config = config;
        this.name = name;
        this.threadLocalSession = new ThreadLocal<>();
        this.sessionCount = new AtomicInteger(0);
    }

    public HugeConfig config() {
        return this.config;
    }

    public final BackendSession getOrNewSession() {
        BackendSession session = this.threadLocalSession.get();
        if (session == null) {
            session = this.newSession();
            assert session != null;
            this.threadLocalSession.set(session);
            this.sessionCount.incrementAndGet();
            LOG.debug("Now(after connect({})) session count is: {}",
                      this, this.sessionCount.get());
        } else {
            this.detectSession(session);
        }
        return session;
    }

    public BackendSession useSession() {
        BackendSession session = this.threadLocalSession.get();
        if (session != null) {
            session.attach();
            this.detectSession(session);
        } else {
            session = this.getOrNewSession();
        }
        return session;
    }

    private void detectSession(BackendSession session) {
        // Reconnect if the session idle time exceed specified value
        long interval = this.config.get(CoreOptions.CONNECTION_DETECT_INTERVAL);
        long now = System.currentTimeMillis();
        if (now - session.updated() > TimeUnit.SECONDS.toMillis(interval)) {
            session.reconnectIfNeeded();
        }
        session.update();
    }

    public Pair<Integer, Integer> closeSession() {
        BackendSession session = this.threadLocalSession.get();
        if (session == null) {
            LOG.warn("Current session has ever been closed");
            return Pair.of(this.sessionCount.get(), -1);
        }

        int ref = session.detach();
        assert ref >= 0 : ref;
        if (ref > 0) {
            return Pair.of(this.sessionCount.get(), ref);
        }

        // Close session when ref=0
        try {
            session.close();
        } catch (Throwable e) {
            session.attach();
            throw e;
        }
        this.threadLocalSession.remove();
        return Pair.of(this.sessionCount.decrementAndGet(), ref);
    }

    public void close() {
        Pair<Integer, Integer> result = Pair.of(-1, -1);
        try {
            result = this.closeSession();
        } finally {
            if (result.getLeft() == 0) {
                this.doClose();
            }
        }
        LOG.debug("Now(after close({})) session count is: {}, " +
                  "current session reference is: {}",
                  this, result.getLeft(), result.getRight());
    }

    public boolean closed() {
        return this.sessionCount.get() == 0;
    }

    @Override
    public String toString() {
        return String.format("%s-%s@%08X", this.name,
                             this.getClass().getSimpleName(), this.hashCode());
    }

    public abstract void open() throws Exception;

    protected abstract boolean opened();

    public abstract BackendSession session();

    protected abstract BackendSession newSession();

    protected abstract void doClose();
}
