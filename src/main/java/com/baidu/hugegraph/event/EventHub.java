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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;

import com.baidu.hugegraph.type.ExtendableIterator;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class EventHub {

    private static final Logger LOG = Log.logger(EventHub.class);

    public static final String ANY_EVENT = "*";

    // Event executor
    private static ExecutorService executor = null;

    private String name;
    private Map<String, List<EventListener>> listeners;

    public EventHub() {
        this("hub");
    }

    public EventHub(String name) {
        this.name = name;
        this.listeners = new ConcurrentHashMap<>();
        EventHub.init(1);
    }

    public static synchronized void init(int poolSize) {
        if (executor != null) {
            return;
        }
        executor = Executors.newFixedThreadPool(poolSize);
    }

    public static synchronized boolean destroy(long timeout)
                                               throws InterruptedException {
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
        return this.listeners.containsKey(event);
    }

    public List<EventListener> listeners(String event) {
        return Collections.unmodifiableList(this.listeners.get(event));
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
        return Collections.unmodifiableList(this.listeners.remove(event));
    }

    public boolean unlisten(String event, EventListener listener) {
        List<EventListener> ls = this.listeners.get(event);
        if (ls == null) {
            return false;
        }
        return ls.remove(listener);
    }

    public void notify(String event, @Nullable Object... args) {
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
            return;
        }

        Event ev = new Event(this, event, args);
        executor().submit(() -> {
            // Notify all listeners, and ignore the results
            while (all.hasNext()) {
                try {
                    all.next().event(ev);
                } catch (Throwable ignored) {
                    LOG.warn("Failed to handle event: {}", ev, ignored);
                }
            }
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
