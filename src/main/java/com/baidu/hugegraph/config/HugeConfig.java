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

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.Iterator;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class HugeConfig extends PropertiesConfiguration {

    private static final Logger LOG = Log.logger(HugeConfig.class);

    public HugeConfig(Configuration config) {
        if (config == null) {
            throw new ConfigException("The config object is null");
        }
        if (config instanceof AbstractFileConfiguration) {
            File file = ((AbstractFileConfiguration) config).getFile();
            if (file != null) {
                this.setFile(file);
            }
        }

        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            this.setProperty(key.replace("..", "."), config.getProperty(key));
        }

        updateDefaultOption();
    }

    public HugeConfig(String configFile) throws ConfigurationException {
        super(loadConfigFile(configFile));
        updateDefaultOption();
    }

    public HugeConfig(InputStream is) throws ConfigurationException {
        E.checkNotNull(is, "config input stream");
        this.load(new InputStreamReader(is));
        updateDefaultOption();
    }

    private static File loadConfigFile(String fileName) {
        E.checkNotNull(fileName, "config file");
        E.checkArgument(!fileName.isEmpty(),
                        "The config file can't be empty");

        File file = new File(fileName);
        E.checkArgument(file.exists() && file.isFile() && file.canRead(),
                        "Need to specify a readable config file, but got: %s",
                        file.toString());
        return file;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void updateDefaultOption() {
        try {
            Iterator<String> keys = this.getKeys();
            while (keys.hasNext()) {
                String key = keys.next();

                if (!OptionSpace.containKey(key)) {
                    LOG.warn("The option '{}' is redundant", key);
                    continue;
                }
                ConfigOption option = OptionSpace.get(key);
                Class<?> dataType = option.dataType();
                String methodGetter = "get" + dataType.getSimpleName();
                Method method = this.getClass().getMethod(methodGetter,
                                                          String.class,
                                                          dataType);
                option.value(method.invoke(this, key, option.value()));
            }
        } catch (Exception e) {
            LOG.error("Failed to update options value: {}", e.getMessage());
            throw new ConfigException("Failed to update options value");
        }
    }

    public <T> T get(ConfigOption<T> option) {
        return option.value();
    }

    @Override
    public void addProperty(String key, Object value) {
        if (value instanceof String) {
            String val = (String) value;
            if (val.startsWith("[") && val.endsWith("]")) {
                val = val.substring(1, val.length() - 1);
            }
            value = val;
        }
        super.addProperty(key, value);
    }
}
