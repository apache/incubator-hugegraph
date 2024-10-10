/*
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

package org.apache.hugegraph.util;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;

public class OrderLimitMap<K extends Comparable<K>, V extends Comparable<V>> extends TreeMap<K, V> {

    private static final long serialVersionUID = 756490437953358633L;

    private final int capacity;
    private final Map<K, V> valueMap;

    private static <V extends Comparable<V>> Ordering<? super V> incr() {
        return Ordering.from(Comparable::compareTo);
    }

    private static <V extends Comparable<V>> Ordering<? super V> decr() {
        return Ordering.from((V o1, V o2) -> -o1.compareTo(o2));
    }

    public OrderLimitMap(int capacity) {
        this(capacity, false);
    }

    public OrderLimitMap(int capacity, boolean incr) {
        this(capacity, incr ? incr() : decr(), new HashMap<>());
    }

    private OrderLimitMap(int capacity, Ordering<? super V> ordering,
                          HashMap<K, V> valueMap) {
        /*
         * onResultOf: for getting the value for the key from value map
         * compound: keep insertion order
         */
        super(ordering.onResultOf(Functions.forMap(valueMap))
                      .compound(Ordering.natural()));
        E.checkArgument(capacity > 0, "The capacity must be > 0");
        this.capacity = capacity;
        this.valueMap = valueMap;
    }

    @Override
    public V put(K k, V v) {
        if (this.valueMap.containsKey(k)) {
            super.remove(k);
        } else if (this.valueMap.size() >= this.capacity) {
            K key = super.lastKey();
            super.remove(key);
            this.valueMap.remove(key);
        }
        this.valueMap.put(k, v);
        return super.put(k, v);
    }

    @Override
    public V get(Object key) {
        return this.valueMap.get(key);
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return this.valueMap.getOrDefault(key, defaultValue);
    }

    @Override
    public boolean containsKey(Object key) {
        return this.valueMap.containsKey(key);
    }

    public Map<K, V> topN(int n) {
        E.checkArgument(n > 0, "'N' Must be positive, but got '%s'", n);
        Map<K, V> top = InsertionOrderUtil.newMap();
        int i = 0;
        for (Map.Entry<K, V> entry : this.entrySet()) {
            top.put(entry.getKey(), entry.getValue());
            if (++i >= n) {
                break;
            }
        }
        return top;
    }
}
