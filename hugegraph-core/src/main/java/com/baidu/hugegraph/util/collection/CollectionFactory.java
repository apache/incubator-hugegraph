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
import com.baidu.hugegraph.type.define.CollectionImplType;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

public class CollectionFactory {

    private CollectionImplType type;

    public CollectionFactory() {
        this.type = CollectionImplType.EC;
    }

    public CollectionFactory(CollectionImplType type) {
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

    public static <V> List<V> newList(CollectionImplType type) {
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

    public static <V> List<V> newList(CollectionImplType type,
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

    public static <V> List<V> newList(CollectionImplType type,
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

    public static <V> Set<V> newSet(CollectionImplType type) {
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

    public static <V> Set<V> newSet(CollectionImplType type,
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

    public static <V> Set<V> newSet(CollectionImplType type,
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

    public static <K, V> Map<K, V> newMap(CollectionImplType type) {
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

    public static <K, V> Map<K, V> newMap(CollectionImplType type,
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

    public Set<Id> newIdSet() {
        return newIdSet(this.type);
    }

    public static Set<Id> newIdSet(CollectionImplType type) {
        return new IdSet(type);
    }
}
