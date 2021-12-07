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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public final class OptionSpace {

    private static final Logger LOG = Log.logger(OptionSpace.class);

    private static final Map<String, Class<? extends OptionHolder>> HOLDERS;
    private static final Map<String, TypedOption<?, ?>> OPTIONS;
    private static final String INSTANCE_METHOD = "instance";

    static {
        HOLDERS = new ConcurrentHashMap<>();
        OPTIONS = new ConcurrentHashMap<>();
    }

    public static void register(String module, String holder) {
        ClassLoader classLoader = OptionSpace.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(holder);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(
                      "Failed to load class of option holder '%s'", e, holder);
        }

        // Check subclass
        if (!OptionHolder.class.isAssignableFrom(clazz)) {
            throw new ConfigException(
                      "Class '%s' is not a subclass of OptionHolder", holder);
        }

        OptionHolder instance = null;
        Exception exception = null;
        try {
            Method method = clazz.getMethod(INSTANCE_METHOD);
            if (!Modifier.isStatic(method.getModifiers())) {
                throw new NoSuchMethodException(INSTANCE_METHOD);
            }
            instance = (OptionHolder) method.invoke(null);
            if (instance == null) {
                exception = new ConfigException(
                                "Returned null from %s() method",
                                INSTANCE_METHOD);
            }
        } catch (NoSuchMethodException e) {
            LOG.warn("Class {} does not has static method {}.",
                     holder, INSTANCE_METHOD);
            exception = e;
        } catch (InvocationTargetException e) {
            LOG.warn("Can't call static method {} from class {}.",
                     INSTANCE_METHOD, holder);
            exception = e;
        } catch (IllegalAccessException e) {
            LOG.warn("Illegal access while calling method {} from class {}.",
                     INSTANCE_METHOD, holder);
            exception = e;
        }

        if (exception != null) {
            throw new ConfigException("Failed to instantiate option holder: %s",
                                      exception, holder);
        }

        register(module, instance);
    }

    public static void register(String module, OptionHolder holder) {
        // Check exists
        if (HOLDERS.containsKey(module)) {
            LOG.warn("Already registered option holder: {} ({})",
                     module, HOLDERS.get(module));
        }
        E.checkArgumentNotNull(holder, "OptionHolder can't be null");
        HOLDERS.put(module, holder.getClass());
        OPTIONS.putAll(holder.options());
        LOG.debug("Registered options for OptionHolder: {}",
                  holder.getClass().getSimpleName());
    }

    public static Set<String> keys() {
        return Collections.unmodifiableSet(OPTIONS.keySet());
    }

    public static boolean containKey(String key) {
        return OPTIONS.containsKey(key);
    }

    public static TypedOption<?, ?> get(String key) {
        return OPTIONS.get(key);
    }
}
