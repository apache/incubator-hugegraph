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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.PropertyConverter;
import org.slf4j.Logger;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

public class TypedOption<T, R> {

    private static final Logger LOG = Log.logger(TypedOption.class);

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
                String[].class,
                List.class
        );

        ACCEPTED_DATA_TYPES_STRING = Joiner.on(", ").join(ACCEPTED_DATA_TYPES);
    }

    private final String name;
    private final String desc;
    private final boolean required;
    private final Class<T> dataType;
    private final T defaultValue;
    private final Predicate<T> checkFunc;

    @SuppressWarnings("unchecked")
    public TypedOption(String name, boolean required, String desc,
                       Predicate<T> pred, Class<T> type, T value) {
        E.checkNotNull(name, "name");
        E.checkNotNull(type, "dataType");

        this.name = name;
        this.dataType = (Class<T>) this.checkAndAssignDataType(type);
        this.defaultValue = value;
        this.required = required;
        this.desc = desc;
        this.checkFunc = pred;

        this.check(this.defaultValue);
    }

    private Class<?> checkAndAssignDataType(Class<T> dataType) {
        for (Class<?> clazz : ACCEPTED_DATA_TYPES) {
            if (clazz.isAssignableFrom(dataType)) {
                return clazz;
            }
        }

        String msg = String.format("Input data type '%s' doesn't belong " +
                                   "to acceptable type set: [%s]",
                                   dataType, ACCEPTED_DATA_TYPES_STRING);
        throw new IllegalArgumentException(msg);
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

    public boolean required() {
        return this.required;
    }

    public R defaultValue() {
        return this.convert(this.defaultValue);
    }

    public R parseConvert(Object value) {
        T parsed = this.parse(value);
        this.check(parsed);
        return this.convert(parsed);
    }

    @SuppressWarnings("unchecked")
    protected T parse(Object value) {
        return (T) this.parse(value, this.dataType);
    }

    protected Object parse(Object value, Class<?> dataType) {
        if (dataType.equals(String.class)) {
            return value;
        } else if (List.class.isAssignableFrom(dataType)) {
            E.checkState(this.forList(),
                         "List option can't be registered with class %s",
                         this.getClass().getSimpleName());
        }

        // Use PropertyConverter method `toXXX` convert value
        String methodTo = "to" + dataType.getSimpleName();
        try {
            Method method = PropertyConverter.class.getMethod(
                            methodTo, Object.class);
            return method.invoke(null, value);
        } catch (ReflectiveOperationException e) {
            LOG.error("Invalid type of value '{}' for option '{}'",
                      value, this.name, e);
            throw new ConfigException(
                      "Invalid type of value '%s' for option '%s', " +
                      "expect '%s' type",
                      value, this.name, dataType.getSimpleName());
        }
    }

    protected void check(Object value) {
        E.checkNotNull(value, "value", this.name);
        if (!this.dataType.isInstance(value)) {
            throw new ConfigException(
                      "Invalid type of value '%s' for option '%s', " +
                      "expect type %s but got %s", value, this.name,
                      this.dataType.getSimpleName(),
                      value.getClass().getSimpleName());
        }

        if (this.checkFunc != null) {
            @SuppressWarnings("unchecked")
            T result = (T) value;
            if (!this.checkFunc.apply(result)) {
                throw new ConfigException("Invalid option value for '%s': %s",
                                          this.name, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected R convert(T value) {
        return (R) value;
    }

    protected boolean forList() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("[%s]%s=%s", this.dataType.getSimpleName(),
                             this.name, this.defaultValue);
    }
}
