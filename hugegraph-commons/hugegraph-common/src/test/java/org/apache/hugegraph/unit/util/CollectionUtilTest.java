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

package org.apache.hugegraph.unit.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class CollectionUtilTest extends BaseUnitTest {

    @Test
    public void testToSet() {
        Assert.assertThrows(NullPointerException.class, () -> {
            CollectionUtil.toSet(null);
        });

        Object array1 = new Integer[]{1, 2, 3};
        Assert.assertEquals(ImmutableSet.of(1, 2, 3),
                            CollectionUtil.toSet(array1));

        Object array2 = new String[]{"1", "2", "3"};
        Assert.assertEquals(ImmutableSet.of("1", "2", "3"),
                            CollectionUtil.toSet(array2));

        Set<Integer> set = ImmutableSet.of(1, 2, 3);
        Assert.assertEquals(ImmutableSet.of(1, 2, 3),
                            CollectionUtil.toSet(set));

        List<Integer> list = ImmutableList.of(1, 2, 3);
        Assert.assertEquals(ImmutableSet.of(1, 2, 3),
                            CollectionUtil.toSet(list));

        Assert.assertEquals(ImmutableSet.of(1), CollectionUtil.toSet(1));
    }

    @Test
    public void testToList() {
        Assert.assertThrows(NullPointerException.class, () -> {
            CollectionUtil.toList(null);
        });

        Object array1 = new Integer[]{1, 2, 3};
        Assert.assertEquals(ImmutableList.of(1, 2, 3),
                            CollectionUtil.toList(array1));

        Object array2 = new String[]{"1", "2", "3"};
        Assert.assertEquals(ImmutableList.of("1", "2", "3"),
                            CollectionUtil.toList(array2));

        Set<Integer> set = ImmutableSet.of(1, 2, 3);
        Assert.assertEquals(ImmutableList.of(1, 2, 3),
                            CollectionUtil.toList(set));

        List<Integer> list = ImmutableList.of(1, 2, 3);
        Assert.assertEquals(ImmutableList.of(1, 2, 3),
                            CollectionUtil.toList(list));

        Assert.assertEquals(ImmutableList.of("123"),
                            CollectionUtil.toList("123"));
    }

    @Test
    public void testPrefixOf() {
        List<Integer> list = ImmutableList.of(1, 2, 3);

        List<Integer> list1 = ImmutableList.of();
        Assert.assertTrue(CollectionUtil.prefixOf(list1, list));

        List<Integer> list2 = ImmutableList.of(1, 2);
        Assert.assertTrue(CollectionUtil.prefixOf(list2, list));

        List<Integer> list3 = ImmutableList.of(1, 2, 3);
        Assert.assertTrue(CollectionUtil.prefixOf(list3, list));

        List<Integer> list4 = ImmutableList.of(1, 2, 3, 4);
        Assert.assertFalse(CollectionUtil.prefixOf(list4, list));

        List<Integer> list5 = ImmutableList.of(1, 2, 4);
        Assert.assertFalse(CollectionUtil.prefixOf(list5, list));

        List<Integer> list6 = Arrays.asList(1, 2, null);
        Assert.assertFalse(CollectionUtil.prefixOf(list6, list));
    }

    @Test
    public void testRandomSet() {
        Set<Integer> set = CollectionUtil.randomSet(0, 100, 10);
        for (int i : set) {
            Assert.assertTrue(0 <= i && i < 100);
        }

        // invalid min
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            CollectionUtil.randomSet(200, 100, 10);
        });

        // invalid count = 0
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            CollectionUtil.randomSet(1, 100, 0);
        });

        // invalid count > max - min
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            CollectionUtil.randomSet(1, 100, 100);
        });
    }

    @Test
    public void testAllUnique() {
        List<Integer> list = ImmutableList.of();
        Assert.assertTrue(CollectionUtil.allUnique(list));

        list = ImmutableList.of(1, 2, 3, 2, 3);
        Assert.assertFalse(CollectionUtil.allUnique(list));

        list = ImmutableList.of(1, 2, 3, 4, 5);
        Assert.assertTrue(CollectionUtil.allUnique(list));

        list = ImmutableList.of(1, 1, 1, 1, 1);
        Assert.assertFalse(CollectionUtil.allUnique(list));
    }

    @Test
    public void testSubSet() {
        Set<Integer> originSet = ImmutableSet.of(1, 2, 3, 4, 5);

        Set<Integer> subSet = CollectionUtil.subSet(originSet, 1, 1);
        Assert.assertEquals(ImmutableSet.of(), subSet);

        subSet = CollectionUtil.subSet(originSet, 2, 4);
        Assert.assertEquals(ImmutableSet.of(3, 4), subSet);

        subSet = CollectionUtil.subSet(originSet, 2, 5);
        Assert.assertEquals(ImmutableSet.of(3, 4, 5), subSet);

        subSet = CollectionUtil.subSet(originSet, 0, 5);
        Assert.assertEquals(ImmutableSet.of(1, 2, 3, 4, 5), subSet);

        subSet = CollectionUtil.subSet(originSet, 2, -1);
        Assert.assertEquals(ImmutableSet.of(3, 4, 5), subSet);

        subSet = CollectionUtil.subSet(originSet, 2, -100);
        Assert.assertEquals(ImmutableSet.of(3, 4, 5), subSet);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            CollectionUtil.subSet(originSet, 2, 1);
        }, e -> {
            Assert.assertContains("Invalid to parameter ", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            CollectionUtil.subSet(originSet, -1, 2);
        }, e -> {
            Assert.assertContains("Invalid from parameter ", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            CollectionUtil.subSet(originSet, -10, 2);
        }, e -> {
            Assert.assertContains("Invalid from parameter ", e.getMessage());
        });
    }

    @Test
    public void testUnion() {
        List<Integer> first = new ArrayList<>();
        first.add(1);
        first.add(2);

        Set<Integer> second = new HashSet<>();
        second.add(1);
        second.add(3);

        Set<Integer> results = CollectionUtil.union(first, second);
        Assert.assertEquals(3, results.size());
    }

    @Test
    public void testIntersectWithoutModifying() {
        List<Integer> first = new ArrayList<>();
        first.add(1);
        first.add(2);
        first.add(3);

        List<Integer> second = new ArrayList<>();
        second.add(4);
        second.add(5);

        Collection<Integer> results = CollectionUtil.intersect(first, second);
        Assert.assertEquals(0, results.size());
        Assert.assertEquals(3, first.size());

        second.add(3);
        results = CollectionUtil.intersect(first, second);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(3, first.size());

        second.add(1);
        second.add(2);
        results = CollectionUtil.intersect(first, second);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(3, first.size());

        Set<Integer> set = new HashSet<>();
        set.add(1);
        set.add(3);
        set.add(6);

        results = CollectionUtil.intersect(set, second);
        Assert.assertInstanceOf(HashSet.class, results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(3, set.size());

        set = new LinkedHashSet<>();
        set.add(1);
        set.add(2);
        set.add(6);

        results = CollectionUtil.intersect(set, second);
        Assert.assertInstanceOf(LinkedHashSet.class, results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(3, set.size());
    }

    @Test
    public void testIntersectWithModifying() {
        Set<Integer> first = new HashSet<>();
        first.add(1);
        first.add(2);
        first.add(3);

        Set<Integer> second = new HashSet<>();
        second.add(1);
        second.add(2);
        second.add(3);

        Collection<Integer> results = CollectionUtil.intersectWithModify(
                                                     first, second);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(3, first.size());

        // The second set has "1", "2"
        second.remove(3);
        results = CollectionUtil.intersectWithModify(first, second);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(2, first.size());

        // The second set is empty
        second.remove(1);
        second.remove(2);
        results = CollectionUtil.intersectWithModify(first, second);
        Assert.assertEquals(0, results.size());
        Assert.assertEquals(0, first.size());
    }

    @Test
    public void testHasIntersectionBetweenListAndSet() {
        List<Integer> first = new ArrayList<>();
        first.add(1);
        first.add(2);
        first.add(3);

        Set<Integer> second = new HashSet<>();
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(4);
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(1);
        Assert.assertTrue(CollectionUtil.hasIntersection(first, second));

        second = new HashSet<>();
        second.add(4);
        second.add(5);
        second.add(6);
        second.add(7);
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(3);
        Assert.assertTrue(CollectionUtil.hasIntersection(first, second));
    }

    @Test
    public void testHasIntersectionBetweenSetAndSet() {
        Set<Integer> first = new HashSet<>();
        first.add(1);
        first.add(2);
        first.add(3);

        Set<Integer> second = new HashSet<>();
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(4);
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(1);
        Assert.assertTrue(CollectionUtil.hasIntersection(first, second));

        second = new HashSet<>();
        second.add(4);
        second.add(5);
        second.add(6);
        second.add(7);
        Assert.assertFalse(CollectionUtil.hasIntersection(first, second));

        second.add(3);
        Assert.assertTrue(CollectionUtil.hasIntersection(first, second));
    }

    @Test
    public void testMapSortByStringKey() {
        Map<String, Integer> unordered = new HashMap<>();
        unordered.put("D", 1);
        unordered.put("B", 2);
        unordered.put("E", 3);
        unordered.put("A", 4);
        unordered.put("C", 5);

        Map<String, Integer> incrOrdered = CollectionUtil.sortByKey(unordered,
                                                                    true);
        Assert.assertEquals(ImmutableList.of("A", "B", "C", "D", "E"),
                            ImmutableList.copyOf(incrOrdered.keySet()));

        Map<String, Integer> decrOrdered = CollectionUtil.sortByKey(unordered,
                                                                    false);
        Assert.assertEquals(ImmutableList.of("E", "D", "C", "B", "A"),
                            ImmutableList.copyOf(decrOrdered.keySet()));
    }

    @Test
    public void testMapSortByIntegerKey() {
        Map<Integer, String> unordered = new HashMap<>();
        unordered.put(4, "A");
        unordered.put(2, "B");
        unordered.put(5, "C");
        unordered.put(1, "D");
        unordered.put(3, "E");

        Map<Integer, String> incrOrdered = CollectionUtil.sortByKey(unordered,
                                                                    true);
        Assert.assertEquals(ImmutableList.of(1, 2, 3, 4, 5),
                            ImmutableList.copyOf(incrOrdered.keySet()));

        Map<Integer, String> decrOrdered = CollectionUtil.sortByKey(unordered,
                                                                    false);
        Assert.assertEquals(ImmutableList.of(5, 4, 3, 2, 1),
                            ImmutableList.copyOf(decrOrdered.keySet()));
    }

    @Test
    public void testMapSortByIntegerValue() {
        Map<String, Integer> unordered = new HashMap<>();
        unordered.put("A", 4);
        unordered.put("B", 2);
        unordered.put("C", 5);
        unordered.put("D", 1);
        unordered.put("E", 3);

        Map<String, Integer> incrOrdered = CollectionUtil.sortByValue(unordered,
                                                                      true);
        Assert.assertEquals(ImmutableList.of(1, 2, 3, 4, 5),
                            ImmutableList.copyOf(incrOrdered.values()));

        Map<String, Integer> decrOrdered = CollectionUtil.sortByValue(unordered,
                                                                      false);
        Assert.assertEquals(ImmutableList.of(5, 4, 3, 2, 1),
                            ImmutableList.copyOf(decrOrdered.values()));
    }

    @Test
    public void testMapSortByStringValue() {
        Map<Integer, String> unordered = new HashMap<>();
        unordered.put(1, "D");
        unordered.put(2, "B");
        unordered.put(3, "E");
        unordered.put(4, "A");
        unordered.put(5, "C");

        Map<Integer, String> incrOrdered = CollectionUtil.sortByValue(unordered,
                                                                      true);
        Assert.assertEquals(ImmutableList.of("A", "B", "C", "D", "E"),
                            ImmutableList.copyOf(incrOrdered.values()));

        Map<Integer, String> decrOrdered = CollectionUtil.sortByValue(unordered,
                                                                      false);
        Assert.assertEquals(ImmutableList.of("E", "D", "C", "B", "A"),
                            ImmutableList.copyOf(decrOrdered.values()));
    }

    @Test
    public void testCrossCombineParts() {
        List<List<Object>> parts;

        parts = ImmutableList.of(ImmutableList.of("a", "b"),
                                 ImmutableList.of(1, 2),
                                 ImmutableList.of('x', 'y'));
        List<List<Object>> combs = CollectionUtil.crossCombineParts(parts);
        Assert.assertEquals(8, combs.size());
        Assert.assertEquals(ImmutableList.of(
                            ImmutableList.of("a", "b", 1, 2, 'x', 'y'),
                            ImmutableList.of("a", "b", 1, 2, 'y', 'x'),
                            ImmutableList.of("a", "b", 2, 1, 'x', 'y'),
                            ImmutableList.of("a", "b", 2, 1, 'y', 'x'),
                            ImmutableList.of("b", "a", 1, 2, 'x', 'y'),
                            ImmutableList.of("b", "a", 1, 2, 'y', 'x'),
                            ImmutableList.of("b", "a", 2, 1, 'x', 'y'),
                            ImmutableList.of("b", "a", 2, 1, 'y', 'x')),
                            combs);

        parts = ImmutableList.of(ImmutableList.of("a", "b", "c"),
                                 ImmutableList.of(1, 2),
                                 ImmutableList.of('x', 'y'));
        Assert.assertEquals(24,
                            CollectionUtil.crossCombineParts(parts).size());

        parts = ImmutableList.of(ImmutableList.of("a", "b", "c"),
                                 ImmutableList.of(1, 2, 3),
                                 ImmutableList.of('x', 'y'));
        Assert.assertEquals(72,
                            CollectionUtil.crossCombineParts(parts).size());

        parts = ImmutableList.of(ImmutableList.of("a", "b", "c"),
                                 ImmutableList.of(1, 2, 3),
                                 ImmutableList.of('x', 'y', 'z'));
        Assert.assertEquals(216,
                            CollectionUtil.crossCombineParts(parts).size());
    }

    @Test
    public void testCnm() {
        List<Integer> list = ImmutableList.of(1, 2, 3, 4, 5);

        // Test C(5, 2) with all combinations
        List<List<Integer>> tuples = new ArrayList<>();
        boolean found;
        found = CollectionUtil.cnm(list, list.size(), 2, tuple -> {
            tuples.add(new ArrayList<>(tuple));
            return false;
        });
        Assert.assertFalse(found);
        Assert.assertEquals(10, tuples.size());

        Assert.assertEquals(10,
                            CollectionUtil.cnm(list, list.size(), 2).size());

        // Test C(5, 2) with one combination
        tuples.clear();
        found = CollectionUtil.cnm(list, list.size(), 2, tuple -> {
            if (tuple.equals(ImmutableList.of(2, 3))) {
                tuples.add(new ArrayList<>(tuple));
                return true;
            }
            return false;
        });
        Assert.assertTrue(found);
        Assert.assertEquals(1, tuples.size());
        Assert.assertEquals(ImmutableList.of(2, 3), tuples.get(0));

        // Test C(5, 3) with all combinations
        List<List<Integer>> triples = new ArrayList<>();
        found = CollectionUtil.cnm(list, list.size(), 3, triple -> {
            triples.add(new ArrayList<>(triple));
            return false;
        });
        Assert.assertFalse(found);
        Assert.assertEquals(10, triples.size());

        Assert.assertEquals(10,
                            CollectionUtil.cnm(list, list.size(), 3).size());

        // Test C(5, 3) with one combination
        triples.clear();
        found = CollectionUtil.cnm(list, list.size(), 3, triple -> {
            if (triple.equals(ImmutableList.of(2, 3, 5))) {
                triples.add(new ArrayList<>(triple));
                return true;
            }
            return false;
        });
        Assert.assertTrue(found);
        Assert.assertEquals(1, triples.size());
        Assert.assertEquals(ImmutableList.of(2, 3, 5), triples.get(0));
    }

    @Test
    public void testAnm() {
        List<Integer> list = ImmutableList.of(1, 2, 3, 4, 5);

        // Test A(5, 5) with all combinations
        Assert.assertEquals(120, CollectionUtil.anm(list).size());

        // Test A(5, 2) with all combinations
        List<List<Integer>> tuples = new ArrayList<>();
        boolean found;
        found = CollectionUtil.anm(list, list.size(), 2, tuple -> {
            tuples.add(new ArrayList<>(tuple));
            return false;
        });
        Assert.assertFalse(found);
        Assert.assertEquals(20, tuples.size());

        Assert.assertEquals(20,
                            CollectionUtil.anm(list, list.size(), 2).size());

        // Test A(5, 2) with one combination
        tuples.clear();
        found = CollectionUtil.anm(list, list.size(), 2, tuple -> {
            if (tuple.equals(ImmutableList.of(2, 3))) {
                tuples.add(new ArrayList<>(tuple));
                return true;
            }
            return false;
        });
        Assert.assertTrue(found);
        Assert.assertEquals(1, tuples.size());
        Assert.assertEquals(ImmutableList.of(2, 3), tuples.get(0));

        // Test A(5, 3) with all combinations
        List<List<Integer>> triples = new ArrayList<>();
        found = CollectionUtil.anm(list, list.size(), 3, triple -> {
            triples.add(new ArrayList<>(triple));
            return false;
        });
        Assert.assertFalse(found);
        Assert.assertEquals(60, triples.size());

        Assert.assertEquals(60,
                            CollectionUtil.anm(list, list.size(), 3).size());

        // Test A(5, 3) with one combination
        triples.clear();
        found = CollectionUtil.anm(list, list.size(), 3, triple -> {
            if (triple.equals(ImmutableList.of(2, 3, 5))) {
                triples.add(new ArrayList<>(triple));
                return true;
            }
            return false;
        });
        Assert.assertTrue(found);
        Assert.assertEquals(1, triples.size());
        Assert.assertEquals(ImmutableList.of(2, 3, 5), triples.get(0));
    }
}
