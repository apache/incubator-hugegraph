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

package com.baidu.hugegraph.backend.cache;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Cache<K, V> {

    public static final String ACTION_INVALID = "invalid";
    public static final String ACTION_CLEAR = "clear";
    public static final String ACTION_INVALIDED = "invalided";
    public static final String ACTION_CLEARED = "cleared";

    public Object get(K id);

    public Object getOrFetch(K id, Function<K, V> fetcher);

    public boolean containsKey(K id);

    public boolean update(K id, V value);

    public boolean update(K id, V value, long timeOffset);

    public boolean updateIfAbsent(K id, V value);

    public boolean updateIfPresent(K id, V value);

    public void invalidate(K id);

    public void traverse(Consumer<V> consumer);

    public void clear();

    public void expire(long ms);

    public long expire();

    public long tick();

    public long capacity();

    public long size();

    public long hits();

    public long miss();

    public <T> T attachment(T object);

    public <T> T attachment();
}
