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

package com.baidu.hugegraph.util.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.E;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

public class CollectionFactory {

    private final CollectionType type;

    public CollectionFactory() {
        this.type = CollectionType.EC;
    }

    public CollectionFactory(CollectionType type) {
        this.type = type;
    }

    public <V> List<V> newList() {
        return newList(this.type);
    }

    public <V> List<V> newList(int initialCapacity) {
        return newList(this.type, initialCapacity);
    }

    public <V> List<V> newList(Collection<V> collection) {
        return newList(this.type, collection);
    }

    public static <V> List<V> newList(CollectionType type) {
        switch (type) {
            case EC:
                return new FastList<>();
            case JCF:
                return new ArrayList<>();
            case FU:
                return new ObjectArrayList<>();
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    public static <V> List<V> newList(CollectionType type,
                                      int initialCapacity) {
        switch (type) {
            case EC:
                return new FastList<>(initialCapacity);
            case JCF:
                return new ArrayList<>(initialCapacity);
            case FU:
                return new ObjectArrayList<>(initialCapacity);
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    public static <V> List<V> newList(CollectionType type,
                                      Collection<V> collection) {
        switch (type) {
            case EC:
                return new FastList<>(collection);
            case JCF:
                return new ArrayList<>(collection);
            case FU:
                return new ObjectArrayList<>(collection);
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    public <V> Set<V> newSet() {
        return newSet(this.type);
    }

    public <V> Set<V> newSet(int initialCapacity) {
        return newSet(this.type, initialCapacity);
    }

    public <V> Set<V> newSet(Collection<V> collection) {
        return newSet(this.type, collection);
    }

    public static <V> Set<V> newSet(CollectionType type) {
        switch (type) {
            case EC:
                return new UnifiedSet<>();
            case JCF:
                return new HashSet<>();
            case FU:
                return new ObjectOpenHashSet<>();
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    public static <V> Set<V> newSet(CollectionType type,
                                    int initialCapacity) {
        switch (type) {
            case EC:
                return new UnifiedSet<>(initialCapacity);
            case JCF:
                return new HashSet<>(initialCapacity);
            case FU:
                return new ObjectOpenHashSet<>(initialCapacity);
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    public static <V> Set<V> newSet(CollectionType type,
                                    Collection<V> collection) {
        switch (type) {
            case EC:
                return new UnifiedSet<>(collection);
            case JCF:
                return new HashSet<>(collection);
            case FU:
                return new ObjectOpenHashSet<>(collection);
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    public <K, V> Map<K, V> newMap() {
        return newMap(this.type);
    }

    public <K, V> Map<K, V> newMap(int initialCapacity) {
        return newMap(this.type, initialCapacity);
    }

    public static <K, V> Map<K, V> newMap(CollectionType type) {
        /*
         * EC is faster 10%-20% than JCF, and it's more stable & less
         * memory cost(size is bigger, EC is better).
         */
        switch (type) {
            case EC:
                return new UnifiedMap<>();
            case JCF:
                return new HashMap<>();
            case FU:
                return new Object2ObjectOpenHashMap<>();
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    public static <K, V> Map<K, V> newMap(CollectionType type,
                                          int initialCapacity) {
        switch (type) {
            case EC:
                return new UnifiedMap<>(initialCapacity);
            case JCF:
                return new HashMap<>(initialCapacity);
            case FU:
                return new Object2ObjectOpenHashMap<>(initialCapacity);
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    public static <V> MutableIntObjectMap<V> newIntObjectMap() {
        return new IntObjectHashMap<>();
    }

    public static <V> MutableIntObjectMap<V> newIntObjectMap(int initialCapacity) {
        return new IntObjectHashMap<>(initialCapacity);
    }

    public static <V> MutableIntObjectMap<V> newIntObjectMap(
                                             IntObjectMap<? extends V> map) {
        return new IntObjectHashMap<>(map);
    }

    public static <V> MutableIntObjectMap<V> newIntObjectMap(
                                             int key1, V value1) {
        return IntObjectHashMap.newWithKeysValues(key1, value1);
    }

    public static <V> MutableIntObjectMap<V> newIntObjectMap(
                                             int key1, V value1,
                                             int key2, V value2) {
        return IntObjectHashMap.newWithKeysValues(key1, value1, key2, value2);
    }

    public static <V> MutableIntObjectMap<V> newIntObjectMap(
                                             int key1, V value1,
                                             int key2, V value2,
                                             int key3, V value3) {
        return IntObjectHashMap.newWithKeysValues(key1, value1, key2, value2,
                                                  key3, value3);
    }

    @SuppressWarnings("unchecked")
    public static <V> MutableIntObjectMap<V> newIntObjectMap(
                                             int key1, V value1,
                                             int key2, V value2,
                                             int key3, V value3,
                                             Object... objects) {
        IntObjectHashMap<V> map = IntObjectHashMap.newWithKeysValues(
                                  key1, value1, key2, value2, key3, value3);
        E.checkArgument(objects.length % 2 == 0,
                        "Must provide even arguments for " +
                        "CollectionFactory.newIntObjectMap");
        for (int i = 0; i < objects.length; i += 2) {
            map.put((int) objects[i], (V) objects[i + 1]);
        }
        return map;
    }

    public static <V> MutableIntObjectMap<V> newIntObjectMap(Id key1,
                                                             V value1) {
        return IntObjectHashMap.newWithKeysValues((int) key1.asLong(), value1);
    }

    public static <V> MutableIntObjectMap<V> newIntObjectMap(
                                             Id key1, V value1,
                                             Id key2, V value2) {
        return IntObjectHashMap.newWithKeysValues((int) key1.asLong(), value1,
                                                  (int) key2.asLong(), value2);
    }

    public static <V> MutableIntObjectMap<V> newIntObjectMap(
                                             Id key1, V value1,
                                             Id key2, V value2,
                                             Id key3, V value3) {
        return IntObjectHashMap.newWithKeysValues((int) key1.asLong(), value1,
                                                  (int) key2.asLong(), value2,
                                                  (int) key3.asLong(), value3);
    }

    @SuppressWarnings("unchecked")
    public static <V> MutableIntObjectMap<V> newIntObjectMap(
                                             Object... objects) {
        IntObjectHashMap<V> map = IntObjectHashMap.newMap();
        E.checkArgument(objects.length % 2 == 0,
                        "Must provide even arguments for " +
                        "CollectionFactory.newIntObjectMap");
        for (int i = 0; i < objects.length; i+=2) {
            map.put((int) ((Id) objects[i]).asLong(), (V) objects[i + 1]);
        }
        return map;
    }

    public Set<Id> newIdSet() {
        return newIdSet(this.type);
    }

    public static Set<Id> newIdSet(CollectionType type) {
        return new IdSet(type);
    }
}
