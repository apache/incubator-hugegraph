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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.baidu.hugegraph.util.E;
import com.google.common.base.Predicate;

public class ConfigListConvOption<T, R> extends TypedOption<List<T>, List<R>> {

    private final Class<T> elemClass;
    private final Function<T, R> converter;

    @SuppressWarnings("unchecked")
    public ConfigListConvOption(String name, String desc,
                                Predicate<List<T>> pred, Function<T, R> convert,
                                T value) {
        this(name, desc, pred, convert, (Class<T>) value.getClass(), value);
    }

    @SuppressWarnings("unchecked")
    public ConfigListConvOption(String name, String desc,
                                Predicate<List<T>> pred, Function<T, R> convert,
                                Class<T> clazz, T... values) {
        this(name, false, desc, pred, convert, clazz, Arrays.asList(values));
    }

    @SuppressWarnings("unchecked")
    public ConfigListConvOption(String name, boolean required, String desc,
                                Predicate<List<T>> pred, Function<T, R> convert,
                                Class<T> clazz, List<T> values) {
        super(name, required, desc, pred,
              (Class<List<T>>) values.getClass(), values);
        E.checkArgumentNotNull(clazz, "Element class can't be null");
        this.elemClass = clazz;
        this.converter = convert;
    }

    @Override
    public List<R> convert(Object value) {
        List<T> results = ConfigListOption.convert(value, part -> {
            return super.convert(part, this.elemClass);
        });

        List<R> enums = new ArrayList<>(results.size());
        for (T elem : results) {
            enums.add(this.converter.apply(elem));
        }
        return enums;
    }
}
