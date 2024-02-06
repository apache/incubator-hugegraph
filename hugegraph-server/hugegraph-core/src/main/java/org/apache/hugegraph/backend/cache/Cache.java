/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.cache;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Cache<K, V> {

    String ACTION_INVALID = "invalid";
    String ACTION_CLEAR = "clear";
    String ACTION_INVALIDED = "invalided";
    String ACTION_CLEARED = "cleared";

    V get(K id);

    V getOrFetch(K id, Function<K, V> fetcher);

    boolean containsKey(K id);

    boolean update(K id, V value);

    boolean update(K id, V value, long timeOffset);

    boolean updateIfAbsent(K id, V value);

    boolean updateIfPresent(K id, V value);

    void invalidate(K id);

    void traverse(Consumer<V> consumer);

    void clear();

    void expire(long ms);

    long expire();

    long tick();

    long capacity();

    long size();

    boolean enableMetrics(boolean enabled);

    long hits();

    long miss();

    <T> T attachment(T object);

    <T> T attachment();
}
