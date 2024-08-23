/*
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

package org.apache.hugegraph.config;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class OptionHolder {

    private static final Logger LOG = Log.logger(HugeConfig.class);

    protected Map<String, TypedOption<?, ?>> options;

    public OptionHolder() {
        this.options = new HashMap<>();
    }

    protected void registerOptions() {
        for (Field field : this.getClass().getFields()) {
            if (!TypedOption.class.isAssignableFrom(field.getType())) {
                // Skip if not option
                continue;
            }
            try {
                TypedOption<?, ?> option = (TypedOption<?, ?>) field.get(this);
                // Fields of subclass first, don't overwrite by superclass
                this.options.putIfAbsent(option.name(), option);
            } catch (Exception e) {
                LOG.error("Failed to register option: {}", field, e);
                throw new ConfigException(
                          "Failed to register option: %s", field);
            }
        }
    }

    public Map<String, TypedOption<?, ?>> options() {
        return Collections.unmodifiableMap(this.options);
    }
}
