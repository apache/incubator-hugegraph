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

import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class OptionSpace {

    private static final Logger LOG = Log.logger(OptionSpace.class);

    private static final Map<String, Class<? extends OptionHolder>> holders;
    private static final Map<String, ConfigOption<?>> options;

    static {
        holders = new ConcurrentHashMap<>();
        options = new ConcurrentHashMap<>();
    }

    public static void register(String module, String holder) {
        ClassLoader classLoader = OptionSpace.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(holder);
        } catch (Exception e) {
            throw new ConfigException(
                      "Failed to load class of option holder '%s'", e, holder);
        }

        // Check subclass
        if (!OptionHolder.class.isAssignableFrom(clazz)) {
            throw new ConfigException(
                      "Class '%s' is not a subclass of OptionHolder", holder);
        }

        OptionHolder instance;
        try {
            Method method = clazz.getMethod("instance");
            instance = (OptionHolder) method.invoke(null);
        } catch (Exception e) {
            throw new ConfigException(
                      "Failed to instantiate option holder '%s'", e, holder);
        }

        register(module, instance);
    }

    public static void register(String module, OptionHolder holder) {
        // Check exists
        if (holders.containsKey(module)) {
            LOG.warn("Already registered option holder: {} ({})",
                     module, holders.get(module));
        }
        holders.put(module, holder.getClass());
        options.putAll(holder.options());
        LOG.debug("Registered options for OptionHolder: {}",
                  holder.getClass().getSimpleName());
    }

    public static Set<String> keys() {
        return options.keySet();
    }

    public static Boolean containKey(String key) {
        return options.containsKey(key);
    }

    public static ConfigOption<?> get(String key) {
        return options.get(key);
    }
}
