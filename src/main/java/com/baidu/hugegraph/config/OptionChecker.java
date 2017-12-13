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

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Predicate;

public class OptionChecker {

    public static final <O> Predicate<O> disallowEmpty() {
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

    public static final Predicate<Integer> positiveInt() {
        return new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer num) {
                return num != null && num > 0;
            }
        };
    }

    public static final Predicate<Integer> nonNegativeInt() {
        return new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer num) {
                return num != null && num >= 0;
            }
        };
    }

    public static final Predicate<Long> positiveLong() {
        return new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long num) {
                return num != null && num > 0;
            }
        };
    }

    public static final Predicate<Integer> rangeInt(int min, int max) {
        return new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer num) {
                return num != null && num >= min && num <= max;
            }
        };
    }
}
