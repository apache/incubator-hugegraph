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

package com.baidu.hugegraph.event;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;

import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

public class EventHub {

    private static final Logger LOG = Log.logger(EventHub.class);

    public static final String EVENT_WORKER = "event-worker-%d";
    public static final String ANY_EVENT = "*";

    private static final List<EventListener> EMPTY = ImmutableList.of();

    // Event executor
    private static ExecutorService executor = null;

    private String name;
    private Map<String, List<EventListener>> listeners;

    public EventHub() {
        this("hub");
    }

    public EventHub(String name) {
        LOG.debug("Create new EventHub: {}", name);

        this.name = name;
        this.listeners = new ConcurrentHashMap<>();
        EventHub.init(1);
    }

    public static synchronized void init(int poolSize) {
        if (executor != null) {
            return;
        }
        LOG.debug("Init pool(size {}) for EventHub", poolSize);
        executor = ExecutorUtil.newFixedThreadPool(poolSize, EVENT_WORKER);
    }

    public static synchronized boolean destroy(long timeout)
                                               throws InterruptedException {
        LOG.debug("Destroy pool for EventHub");
        executor.shutdown();
        return executor.awaitTermination(timeout, TimeUnit.SECONDS);
    }

    private static ExecutorService executor() {
        ExecutorService e = executor;
        E.checkState(e != null, "The event executor has been destroyed");
        return e;
    }

    public String name() {
        return this.name;
    }

    public boolean containsListener(String event) {
        List<EventListener> ls = this.listeners.get(event);
        return ls != null && ls.size() > 0;
    }

    public List<EventListener> listeners(String event) {
        List<EventListener> ls = this.listeners.get(event);
        return ls == null ? EMPTY : Collections.unmodifiableList(ls);
    }

    public void listen(String event, EventListener listener) {
        E.checkNotNull(event, "event");
        E.checkNotNull(listener, "event listener");

        if (!this.listeners.containsKey(event)) {
            this.listeners.putIfAbsent(event, new CopyOnWriteArrayList<>());
        }
        List<EventListener> ls = this.listeners.get(event);
        assert ls != null : this.listeners;
        ls.add(listener);
    }

    public List<EventListener> unlisten(String event) {
        List<EventListener> ls = this.listeners.remove(event);
        return ls == null ? EMPTY : Collections.unmodifiableList(ls);
    }

    public int unlisten(String event, EventListener listener) {
        List<EventListener> ls = this.listeners.get(event);
        if (ls == null) {
            return 0;
        }

        int count = 0;
        while (ls.remove(listener)) {
            count++;
        }
        return count;
    }

    public Future<Integer> notify(String event, @Nullable Object... args) {
        @SuppressWarnings("resource")
        ExtendableIterator<EventListener> all = new ExtendableIterator<>();

        List<EventListener> ls = this.listeners.get(event);
        if (ls != null && !ls.isEmpty()) {
            all.extend(ls.iterator());
        }
        List<EventListener> lsAny = this.listeners.get(ANY_EVENT);
        if (lsAny != null && !lsAny.isEmpty()) {
            all.extend(lsAny.iterator());
        }

        if (!all.hasNext()) {
            return CompletableFuture.completedFuture(0);
        }

        Event ev = new Event(this, event, args);

        // The submit will catch params: `all`(Listeners) and `ev`(Event)
        return executor().submit(() -> {
            int count = 0;
            // Notify all listeners, and ignore the results
            while (all.hasNext()) {
                try {
                    all.next().event(ev);
                    count++;
                } catch (Throwable ignored) {
                    LOG.warn("Failed to handle event: {}", ev, ignored);
                }
            }
            return count;
        });
    }

    public Object call(String event, @Nullable Object... args) {
        List<EventListener> ls = this.listeners.get(event);
        if (ls == null) {
            throw new RuntimeException("Not found listener for: " + event);
        } else if (ls.size() != 1) {
            throw new RuntimeException("Too many listeners for: " + event);
        }
        EventListener listener = ls.get(0);
        return listener.event(new Event(this, event, args));
    }
}
