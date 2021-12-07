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
                                T... values) {
        this(name, false, desc, pred, convert, null, Arrays.asList(values));
    }

    @SuppressWarnings("unchecked")
    public ConfigListConvOption(String name, boolean required, String desc,
                                Predicate<List<T>> pred, Function<T, R> convert,
                                Class<T> clazz, List<T> values) {
        super(name, required, desc, pred,
              (Class<List<T>>) values.getClass(), values);
        E.checkNotNull(convert, "convert");
        if (clazz == null && values.size() > 0) {
            clazz = (Class<T>) values.get(0).getClass();
        }
        E.checkArgumentNotNull(clazz, "Element class can't be null");
        this.elemClass = clazz;
        this.converter = convert;
    }

    @Override
    protected boolean forList() {
        return true;
    }

    @Override
    protected List<T> parse(String value) {
        return ConfigListOption.convert(value, part -> {
            return this.parse(part, this.elemClass);
        });
    }

    @Override
    public List<R> convert(List<T> values) {
        List<R> results = new ArrayList<>(values.size());
        for (T value : values) {
            results.add(this.converter.apply(value));
        }
        return results;
    }
}
