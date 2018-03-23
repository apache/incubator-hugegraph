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

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;

import java.io.File;
import java.util.Iterator;
import java.util.List;

public class HugeConfig extends PropertiesConfiguration {

    private static final Logger LOG = Log.logger(HugeConfig.class);

    public HugeConfig(Configuration config) {
        if (config == null) {
            throw new ConfigException("The config object is null");
        }

        this.reloadIfNeed(config);

        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            if (key.contains("..")) {
                key = key.replace("..", ".");
            }
            this.addProperty(key, config.getProperty(key));
        }
        this.checkRequiredOptions();
    }

    public HugeConfig(String configFile) {
        this(loadConfigFile(configFile));
    }

    private void reloadIfNeed(Configuration config) {
        if (config instanceof AbstractFileConfiguration) {
            AbstractFileConfiguration fileConfig =
                                      (AbstractFileConfiguration) config;

            File file = fileConfig.getFile();
            if (file != null) {
                // May need to use the original file
                this.setFile(file);
            }

            if (!fileConfig.isDelimiterParsingDisabled()) {
                /*
                 * PropertiesConfiguration will parse the containing comma
                 * config options into list directly, but we want to do
                 * this work by ourselves, so reload it and parse into `String`
                 */
                fileConfig.setDelimiterParsingDisabled(true);
                try {
                    fileConfig.refresh();
                } catch (ConfigurationException e) {
                    throw new ConfigException("Unable to load config file: %s",
                                              e, file);
                }
            }
        }
    }

    private static PropertiesConfiguration loadConfigFile(String path) {
        E.checkNotNull(path, "config path");
        E.checkArgument(!path.isEmpty(),
                        "The config path can't be empty");

        File file = new File(path);
        E.checkArgument(file.exists() && file.isFile() && file.canRead(),
                        "Need to specify a readable config, but got: %s",
                        file.toString());

        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setDelimiterParsingDisabled(true);
        try {
            config.load(file);
        } catch (ConfigurationException e) {
            throw new ConfigException("Unable to load config: %s", e, path);
        }
        return config;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(ConfigOption<T> option) {
        Object value = this.getProperty(option.name());
        return value != null ? (T) value : option.defaultValue();
    }

    @Override
    public void addProperty(String key, Object value) {
        if (!OptionSpace.containKey(key)) {
            LOG.warn("The config option '{}' is redundant, " +
                     "please ensure it has been registered", key);
        } else {
            // The input value is String(parsed by PropertiesConfiguration)
            value = this.validateOption(key, value);
        }
        super.addPropertyDirect(key, value);
    }

    private Object validateOption(String key, Object value) {
        E.checkArgument(value instanceof String,
                        "Invalid value for key '%s': %s", key, value);

        ConfigOption<?> option = OptionSpace.get(key);
        Class<?> dataType = option.dataType();

        if (List.class.isAssignableFrom(dataType)) {
            E.checkState(option instanceof ConfigListOption,
                         "List option must be registered with " +
                         "class ConfigListOption");
        }

        value = option.convert(value);
        option.check(value);
        return value;
    }

    private void checkRequiredOptions() {
        // TODO: Check required options must be contained in this map
    }
}
