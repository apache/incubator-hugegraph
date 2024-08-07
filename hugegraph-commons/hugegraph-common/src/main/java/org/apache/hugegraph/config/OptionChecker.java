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

package org.apache.hugegraph.config;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Predicate;

public final class OptionChecker {

    public static <O> Predicate<O> disallowEmpty() {
        return o -> {
            if (o == null) {
                return false;
            }
            if (o instanceof String) {
                return StringUtils.isNotBlank((String) o);
            }
            if (o.getClass().isArray() && (Array.getLength(o) == 0)) {
                return false;
            }
            return !(o instanceof Iterable) || ((Iterable<?>) o).iterator().hasNext();
        };
    }

    @SuppressWarnings("unchecked")
    public static <O> Predicate<O> allowValues(O... values) {
        return o -> o != null && Arrays.asList(values).contains(o);
    }

    @SuppressWarnings("unchecked")
    public static <O> Predicate<List<O>> inValues(O... values) {
        return o -> o != null && new HashSet<>(Arrays.asList(values)).containsAll(o);
    }

    public static <N extends Number> Predicate<N> positiveInt() {
        return number -> number != null && number.longValue() > 0;
    }

    public static <N extends Number> Predicate<N> nonNegativeInt() {
        return number -> number != null && number.longValue() >= 0;
    }

    public static <N extends Number> Predicate<N> rangeInt(N min, N max) {
        return number -> {
            if (number == null) {
                return false;
            }
            long value = number.longValue();
            return value >= min.longValue() && value <= max.longValue();
        };
    }

    public static <N extends Number> Predicate<N> rangeDouble(N min, N max) {
        return number -> {
            if (number == null) {
                return false;
            }
            double value = number.doubleValue();
            return value >= min.doubleValue() && value <= max.doubleValue();
        };
    }
}
