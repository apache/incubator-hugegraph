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

package com.baidu.hugegraph.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public final class CollectionUtil {

    @SuppressWarnings("unchecked")
    public static <T> List<T> toList(Object array) {
        E.checkNotNull(array, "array");
        E.checkArgument(array.getClass().isArray(),
                        "The parameter of toList() must be an array: '%s'",
                        array.getClass().getSimpleName());

        int length = Array.getLength(array);
        List<T> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add((T) Array.get(array, i));
        }
        return list;
    }

    public static <T> boolean prefixOf(List<T> prefix, List<T> all) {
        E.checkNotNull(prefix, "prefix");
        E.checkNotNull(all, "all");

        if (prefix.size() > all.size()) {
            return false;
        }

        for (int i = 0; i < prefix.size(); i++) {
            T first = prefix.get(i);
            T second = all.get(i);
            if (first == second) {
                continue;
            }
            if (first == null || !first.equals(second)) {
                return false;
            }
        }
        return true;
    }

    public static Set<Integer> randomSet(int min, int max, int count) {
        E.checkArgument(max > min, "Invalid min/max: %s/%s", min, max);
        E.checkArgument(0 < count && count <= max - min,
                        "Invalid count %s", count);

        Set<Integer> randoms = new HashSet<>();
        while (randoms.size() < count) {
            randoms.add(ThreadLocalRandom.current().nextInt(min, max));
        }
        return randoms;
    }

    public static boolean allUnique(Collection<?> collection) {
        return collection.stream().allMatch(new HashSet<>()::add);
    }

    /**
     * Get sub-set of a set.
     * @param original original set
     * @param from index of start position
     * @param to index of end position(exclude), but -1 means the last element
     * @param <T> element type of set
     * @return sub-set of original set [from, to)
     */
    public static <T> Set<T> subSet(Set<T> original, int from, int to) {
        List<T> list = new ArrayList<>(original);
        if (to == -1) {
            to = original.size();
        }
        return new LinkedHashSet<>(list.subList(from, to));
    }

    public static <T> Set<T> union(Collection<T> first, Collection<T> second) {
        E.checkNotNull(first, "first");
        E.checkNotNull(second, "second");
        HashSet<T> results = new HashSet<>(first);
        results.addAll(second);
        return results;
    }

    /**
     * Find the intersection of two collections, the original collections
     * will not be modified
     * @param first the first collection
     * @param second the second collection
     * @param <T> element type of collection
     * @return intersection of the two collections
     */
    public static <T> Collection<T> intersect(Collection<T> first,
                                              Collection<T> second) {
        E.checkNotNull(first, "first");
        E.checkNotNull(second, "second");

        HashSet<T> results = null;
        if (first instanceof HashSet) {
            @SuppressWarnings("unchecked")
            HashSet<T> clone = (HashSet<T>) ((HashSet<T>) first).clone();
            results = clone;
        } else {
            results = new HashSet<>(first);
        }
        results.retainAll(second);
        return results;
    }

    /**
     * Find the intersection of two collections, note that the first collection
     * will be modified and store the results
     * @param first the first collection, it will be modified
     * @param second the second collection
     * @param <T> element type of collection
     * @return intersection of the two collections
     */
    public static <T> Collection<T> intersectWithModify(Collection<T> first,
                                                        Collection<T> second) {
        E.checkNotNull(first, "first");
        E.checkNotNull(second, "second");
        first.retainAll(second);
        return first;
    }

    public static <T> boolean hasIntersection(List<T> first, Set<T> second) {
        E.checkNotNull(first, "first");
        E.checkNotNull(second, "second");
        for (T firstElem : first) {
            if (second.contains(firstElem)) {
                return true;
            }
        }
        return false;
    }

    public static <T> boolean hasIntersection(Set<T> first, Set<T> second) {
        E.checkNotNull(first, "first");
        E.checkNotNull(second, "second");
        if (first.size() <= second.size()) {
            for (T firstElem : first) {
                if (second.contains(firstElem)) {
                    return true;
                }
            }
        } else {
            for (T secondElem : second) {
                if (first.contains(secondElem)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static <K extends Comparable<? super K>, V> Map<K, V> sortByKey(
                  Map<K, V> map, boolean incr) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        if (incr) {
            list.sort(Map.Entry.comparingByKey());
        } else {
            list.sort(Collections.reverseOrder(Map.Entry.comparingByKey()));
        }

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
                  Map<K, V> map, boolean incr) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        if (incr) {
            list.sort(Map.Entry.comparingByValue());
        } else {
            list.sort(Collections.reverseOrder(Map.Entry.comparingByValue()));
        }

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
