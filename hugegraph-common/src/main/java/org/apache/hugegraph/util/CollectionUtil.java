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

package org.apache.hugegraph.util;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public final class CollectionUtil {

    public static <T> Set<T> toSet(Object object) {
        E.checkNotNull(object, "object");
        Set<T> set = InsertionOrderUtil.newSet();
        fillCollection(set, object);
        return set;
    }

    public static <T> List<T> toList(Object object) {
        E.checkNotNull(object, "object");
        List<T> list = new ArrayList<>();
        fillCollection(list, object);
        return list;
    }

    @SuppressWarnings("unchecked")
    private static <T> void fillCollection(Collection<T> collection,
                                           Object object) {
        if (object.getClass().isArray()) {
            int length = Array.getLength(object);
            for (int i = 0; i < length; i++) {
                collection.add((T) Array.get(object, i));
            }
        } else if (object instanceof Collection) {
            collection.addAll((Collection<T>) object);
        } else {
            collection.add((T) object);
        }
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
        HashSet<Object> set = new HashSet<>(collection.size());
        for (Object elem : collection) {
            if (!set.add(elem)) {
                return false;
            }
        }
        return true;
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
        E.checkArgument(from >= 0,
                        "Invalid from parameter of subSet(): %s", from);
        if (to < 0) {
            to = original.size();
        } else {
            E.checkArgument(to >= from,
                            "Invalid to parameter of subSet(): %s", to);
        }
        List<T> list = new ArrayList<>(original);
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

    /**
     * Cross combine the A(n, n) combinations of each part in parts
     * @param parts a list of part, like [{a,b}, {1,2}, {x,y}]
     * @return List<List<Integer>> all combinations like
     * [{a,b,1,2,x,y}, {a,b,1,2,y,x}, {a,b,2,1,x,y}, {a,b,2,1,y,x}...]
     */
    public static <T> List<List<T>> crossCombineParts(List<List<T>> parts) {
        List<List<T>> results = new ArrayList<>();
        Deque<List<T>> selected = new ArrayDeque<>();
        crossCombineParts(parts, 0, selected, results);
        return results;
    }

    private static <T> void crossCombineParts(List<List<T>> parts,
                                              int level,
                                              Deque<List<T>> selected,
                                              List<List<T>> results) {
        assert level < parts.size();
        List<T> part = parts.get(level);
        for (List<T> combination : anm(part)) {
            selected.addLast(combination);

            if (level < parts.size() - 1) {
                crossCombineParts(parts, level + 1, selected, results);
            } else if (level == parts.size() - 1) {
                results.add(flatToList(selected));
            }

            selected.removeLast();
        }
    }

    private static <T> List<T> flatToList(Deque<List<T>> parts) {
        List<T> list = new ArrayList<>();
        for (List<T> part : parts) {
            list.addAll(part);
        }
        return list;
    }

    /**
     * Traverse C(n, m) combinations of a list
     * @param all list to contain all items for combination
     * @param n m of C(n, m)
     * @param m n of C(n, m)
     * @return List<List<T>> all combinations
     */
    public static <T> List<List<T>> cnm(List<T> all, int n, int m) {
        List<List<T>> combs = new ArrayList<>();
        cnm(all, n, m, comb -> {
            combs.add(comb);
            return false;
        });
        return combs;
    }

    /**
     * Traverse C(n, m) combinations of a list to find first matched
     * result combination and call back with the result.
     * @param all list to contain all items for combination
     * @param n m of C(n, m)
     * @param m n of C(n, m)
     * @return true if matched any kind of items combination else false, the
     * callback can always return false if want to traverse all combinations
     */
    public static <T> boolean cnm(List<T> all, int n, int m,
                                  Function<List<T>, Boolean> callback) {
        return cnm(all, n, m, 0, null, callback);
    }

    /**
     * Traverse C(n, m) combinations of a list to find first matched
     * result combination and call back with the result.
     * @param all list to contain all items for combination
     * @param n n of C(n, m)
     * @param m m of C(n, m)
     * @param current current position in list
     * @param selected list to contains selected items
     * @return true if matched any kind of items combination else false, the
     * callback can always return false if want to traverse all combinations
     */
    private static <T> boolean cnm(List<T> all, int n, int m,
                                   int current, List<T> selected,
                                   Function<List<T>, Boolean> callback) {
        assert n <= all.size();
        assert m <= n;
        assert current <= all.size();
        if (selected == null) {
            selected = new ArrayList<>(m);
        }

        if (m == 0) {
            assert selected.size() > 0 : selected;
            // All n items are selected
            List<T> tmpResult = Collections.unmodifiableList(selected);
            return callback.apply(tmpResult);
        }
        if (n == m) {
            // No choice, select all n items, we don't update the `result` here
            List<T> tmpResult = new ArrayList<>(selected);
            tmpResult.addAll(all.subList(current, all.size()));
            return callback.apply(tmpResult);
        }

        if (current >= all.size()) {
            // Reach the end of items
            return false;
        }

        // Select current item, continue to select C(m-1, n-1)
        int index = selected.size();
        selected.add(all.get(current));
        ++current;
        if (cnm(all, n - 1, m - 1, current, selected, callback)) {
            // NOTE: we can pop the tailing items if want to keep result clear
            return true;
        }
        // Not select current item, pop it and continue to select C(m-1, n)
        selected.remove(index);
        assert selected.size() == index : selected;
        if (cnm(all, n - 1, m, current, selected, callback)) {
            return true;
        }

        return false;
    }

    /**
     * Traverse A(n, m) combinations of a list with n = m = all.size()
     * @param all list to contain all items for combination
     * @return List<List<T>> all combinations
     */
    public static <T> List<List<T>> anm(List<T> all) {
        return anm(all, all.size(), all.size());
    }

    /**
     * Traverse A(n, m) combinations of a list
     * @param all list to contain all items for combination
     * @param n m of A(n, m)
     * @param m n of A(n, m)
     * @return List<List<T>> all combinations
     */
    public static <T> List<List<T>> anm(List<T> all, int n, int m) {
        List<List<T>> combs = new ArrayList<>();
        anm(all, n, m, comb -> {
            combs.add(comb);
            return false;
        });
        return combs;
    }

    /**
     * Traverse A(n, m) combinations of a list to find first matched
     * result combination and call back with the result.
     * @param all list to contain all items for combination
     * @param n m of A(n, m)
     * @param m n of A(n, m)
     * @return true if matched any kind of items combination else false, the
     * callback can always return false if want to traverse all combinations
     */
    public static <T> boolean anm(List<T> all, int n, int m,
                                  Function<List<T>, Boolean> callback) {
        return anm(all, n, m, null, callback);
    }

    /**
     * Traverse A(n, m) combinations of a list to find first matched
     * result combination and call back with the result.
     * @param all list to contain all items for combination
     * @param n m of A(n, m)
     * @param m n of A(n, m)
     * @param selected list to contains selected items
     * @return true if matched any kind of items combination else false, the
     * callback can always return false if want to traverse all combinations
     */
    private static <T> boolean anm(List<T> all, int n, int m,
                                   List<Integer> selected,
                                   Function<List<T>, Boolean> callback) {
        assert n <= all.size();
        assert m <= n;
        if (selected == null) {
            selected = new ArrayList<>(m);
        }

        if (m == 0) {
            // All n items are selected
            List<T> tmpResult = new ArrayList<>();
            for (int i : selected) {
                tmpResult.add(all.get(i));
            }
            return callback.apply(tmpResult);
        }

        for (int i = 0; i < all.size(); i++) {
            if (selected.contains(i)) {
                continue;
            }
            int index = selected.size();
            selected.add(i);

            // Select current item, continue to select A(m-1, n-1)
            if (anm(all, n - 1, m - 1, selected, callback)) {
                return true;
            }

            selected.remove(index);
            assert selected.size() == index : selected;
        }
        return false;
    }
}
