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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.config;

import java.util.Set;

import org.slf4j.Logger;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

public class ConfigOption<T> {

    private static final Logger LOG = Log.logger(ConfigOption.class);

    private static final Set<Class<?>> ACCEPTED_DATA_TYPES;
    private static final String ACCEPTED_DATA_TYPES_STRING;

    static {
        ACCEPTED_DATA_TYPES = ImmutableSet.of(
                Boolean.class,
                Short.class,
                Integer.class,
                Byte.class,
                Long.class,
                Float.class,
                Double.class,
                String.class,
                String[].class
        );

        ACCEPTED_DATA_TYPES_STRING = Joiner.on(", ").join(ACCEPTED_DATA_TYPES);
    }

    private final String name;
    private final String desc;
    private final Boolean rewritable;
    private final Class<T> dataType;
    private T value;
    private final Predicate<T> checkFunc;

    @SuppressWarnings("unchecked")
    public ConfigOption(String name, T value, Boolean rewritable,
                        String desc, Predicate<T> func) {
        this(name, (Class<T>) value.getClass(), value, rewritable, desc, func);
    }

    public ConfigOption(String name, Class<T> dataType, T value,
                        Boolean rewritable, String desc, Predicate<T> func) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(dataType);
        Preconditions.checkNotNull(rewritable);

        if (!ACCEPTED_DATA_TYPES.contains(dataType)) {
            String msg = String.format("Input data type '%s' doesn't belong " +
                                       "to acceptable type set: [%s]",
                                       dataType, ACCEPTED_DATA_TYPES_STRING);
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }

        this.name = name;
        this.dataType = dataType;
        this.value = value;
        this.rewritable = rewritable;
        this.desc = desc;
        this.checkFunc = func;

        if (this.checkFunc != null) {
            check(this.value);
        }
    }

    public String name() {
        return this.name;
    }

    public Class<T> dataType() {
        return this.dataType;
    }

    public String desc() {
        return this.desc;
    }

    public T value() {
        return this.value;
    }

    public void value(T value) {
        check(value);
        E.checkArgument(this.rewritable,
                        "Not allowed to modify option '%s' " +
                        "which can't be rewritable", this.name);
        this.value = value;
    }

    public void check(Object value) {
        E.checkNotNull(value, "value", this.name);
        E.checkArgument(this.dataType.isInstance(value),
                        "Invalid class for option '%s', " +
                        "expected '%s' but given '%s'",
                        this.name, this.dataType, value.getClass());
        @SuppressWarnings("unchecked")
        T result = (T) value;
        E.checkArgument(this.checkFunc.apply(result),
                        "Invalid option value for '%s': %s",
                        this.name, value);
    }
}
