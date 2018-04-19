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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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

    public static boolean allUnique(Collection<?> collection){
        return collection.stream().allMatch(new HashSet<>()::add);
    }

    /**
     * Get sub-set of a set.
     * @param original original set
     * @param from index of start position
     * @param to index of end position(exclude), but -1 means the last element
     * @return sub-set of original set [from,to)
     */
    public static <T> Set<T> subSet(Set<T> original, int from, int to) {
        List<T> list = new ArrayList<>(original);
        if (to == -1) {
            to = original.size();
        }
        return new LinkedHashSet<>(list.subList(from, to));
    }
}
