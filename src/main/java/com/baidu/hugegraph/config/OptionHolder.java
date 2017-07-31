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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptionHolder {

    private static final Logger logger =
            LoggerFactory.getLogger(HugeConfig.class);

    protected Map<String, ConfigOption> options;

    public OptionHolder() {
        this.options = new HashMap<>();
    }

    protected void registerOptions() {
        Field[] fields = this.getClass().getFields();
        for (Field field : fields) {
            try {
                ConfigOption option = (ConfigOption) field.get(this);
                this.options.put(option.name(), option);
            } catch (Exception e) {
                String msg = String.format(
                             "Failed to register option : %s", field);
                logger.error(msg, e);
                throw new ConfigException(msg, e);
            }
        }
    }

    public Map<String, ConfigOption> options() {
        return this.options;
    }
}
