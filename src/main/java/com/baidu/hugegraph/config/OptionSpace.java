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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.util.E;
import com.google.common.collect.Maps;

/**
 * Created by liningrui on 2017/3/27.
 */
public class OptionSpace {

    private static final Logger logger =
            LoggerFactory.getLogger(OptionSpace.class);

    private static final Map<String, ConfigOption> options = Maps.newHashMap();

    public static void register(OptionHolder holder) {
        options.putAll(holder.options());
        logger.debug("Registered " + holder.getClass().getSimpleName());
    }

    public static void register(ConfigOption<?> element) {
        E.checkArgument(!options.containsKey(element.name()),
                "The option: '%s' has already been registered",
                element.name());
        options.put(element.name(), element);
    }

    public static Boolean containKey(String key) {
        return options.containsKey(key);
    }

    public static ConfigOption<?> get(String key) {
        return options.get(key);
    }
}
