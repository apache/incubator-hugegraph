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

package com.baidu.hugegraph.config;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Predicate;

public final class OptionChecker {

    public static <O> Predicate<O> disallowEmpty() {
        return new Predicate<O>() {
            @Override
            public boolean apply(@Nullable O o) {
                if (o == null) {
                    return false;
                }
                if (o instanceof String) {
                    return StringUtils.isNotBlank((String) o);
                }
                if (o.getClass().isArray() && (Array.getLength(o) == 0)) {
                    return false;
                }
                if (o instanceof Iterable &&
                    !((Iterable<?>) o).iterator().hasNext()) {
                    return false;
                }
                return true;
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <O> Predicate<O> allowValues(O... values) {
        return new Predicate<O>() {
            @Override
            public boolean apply(@Nullable O o) {
                return o != null && Arrays.asList(values).contains(o);
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <O> Predicate<List<O>> inValues(O... values) {
        return new Predicate<List<O>>() {
            @Override
            public boolean apply(@Nullable List<O> o) {
                return o != null && Arrays.asList(values).containsAll(o);
            }
        };
    }

    public static <N extends Number> Predicate<N> positiveInt() {
        return new Predicate<N>() {
            @Override
            public boolean apply(@Nullable N number) {
                return number != null && number.longValue() > 0;
            }
        };
    }

    public static <N extends Number> Predicate<N> nonNegativeInt() {
        return new Predicate<N>() {
            @Override
            public boolean apply(@Nullable N number) {
                return number != null && number.longValue() >= 0;
            }
        };
    }

    public static <N extends Number> Predicate<N> rangeInt(N min, N max) {
        return new Predicate<N>() {
            @Override
            public boolean apply(@Nullable N number) {
                if (number == null) {
                    return false;
                }
                long value = number.longValue();
                return value >= min.longValue() && value <= max.longValue();
            }
        };
    }

    public static <N extends Number> Predicate<N> rangeDouble(N min, N max) {
        return new Predicate<N>() {
            @Override
            public boolean apply(@Nullable N number) {
                if (number == null) {
                    return false;
                }
                double value = number.doubleValue();
                return value >= min.doubleValue() && value <= max.doubleValue();
            }
        };
    }
}
