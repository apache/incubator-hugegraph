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

import java.util.function.Function;

import com.baidu.hugegraph.util.E;
import com.google.common.base.Predicate;

public class ConfigConvOption<T, R> extends TypedOption<T, R> {

    private final Function<T, R> converter;

    public ConfigConvOption(String name, String desc, Predicate<T> pred,
                            Function<T, R> convert, T value) {
        this(name, false, desc, pred, convert, value);
    }

    @SuppressWarnings("unchecked")
    public ConfigConvOption(String name, boolean required, String desc,
                            Predicate<T> pred, Function<T, R> convert,
                            T value) {
        super(name, required, desc, pred, (Class<T>) value.getClass(), value);
        E.checkNotNull(convert, "convert");
        this.converter = convert;
    }

    @Override
    public R convert(T value) {
        return this.converter.apply(value);
    }
}
