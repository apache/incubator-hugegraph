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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class InsertionOrderUtil {

    public static <K, V> Map<K, V> newMap() {
        return new LinkedHashMap<>();
    }

    public static <K, V> Map<K, V> newMap(int initialCapacity) {
        return new LinkedHashMap<>(initialCapacity);
    }

    public static <K, V> Map<K, V> newMap(Map<K, V> origin) {
        return new LinkedHashMap<>(origin);
    }

    public static <V> Set<V> newSet() {
        return new LinkedHashSet<>();
    }

    public static <V> Set<V> newSet(int initialCapacity) {
        return new LinkedHashSet<>(initialCapacity);
    }

    public static <V> Set<V> newSet(Set<V> origin) {
        return new LinkedHashSet<>(origin);
    }

    public static <V> List<V> newList() {
        return new ArrayList<>();
    }

    public static <V> List<V> newList(int initialCapacity) {
        return new ArrayList<>(initialCapacity);
    }

    public static <V> List<V> newList(List<V> origin) {
        return new ArrayList<>(origin);
    }
}
