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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.annotation.Nullable;

public class HugeConfig extends PropertiesConfiguration {

    private static final Logger LOG = Log.logger(HugeConfig.class);

    private final String path;

    public HugeConfig(Configuration config) {
        loadConfig(config);
        this.path = null;
    }

    public HugeConfig(String configFile) {
        loadConfig(loadConfigFile(configFile));
        this.path = configFile;
    }

    private void loadConfig(Configuration config) {
        if (config == null) {
            throw new ConfigException("The config object is null");
        }
        this.setLayoutIfNeeded(config);

        this.append(config);
        this.checkRequiredOptions();
    }

    private void setLayoutIfNeeded(Configuration conf) {
        if (!(conf instanceof PropertiesConfiguration)) {
            return;
        }
        PropertiesConfiguration propConf = (PropertiesConfiguration) conf;
        this.setLayout(propConf.getLayout());
    }

    private static Configuration loadConfigFile(String path) {
        E.checkNotNull(path, "config path");
        E.checkArgument(!path.isEmpty(),
                        "The config path can't be empty");

        File file = new File(path);
        return loadConfigFile(file);
    }

    @SuppressWarnings("unchecked")
    public <T, R> R get(TypedOption<T, R> option) {
        Object value = this.getProperty(option.name());
        if (value == null) {
            return option.defaultValue();
        }
        return (R) value;
    }

    public Map<String, String> getMap(ConfigListOption<String> option) {
        List<String> values = this.get(option);
        Map<String, String> result = new HashMap<>();
        for (String value : values) {
            String[] pair = value.split(":", 2);
            E.checkState(pair.length == 2,
                         "Invalid option format for '%s': %s(expect KEY:VALUE)",
                         option.name(), value);
            result.put(pair[0].trim(), pair[1].trim());
        }
        return result;
    }

    @Override
    public void addPropertyDirect(String key, Object value) {
        TypedOption<?, ?> option = OptionSpace.get(key);
        if (option == null) {
            LOG.warn("The config option '{}' is redundant, " +
                     "please ensure it has been registered", key);
        } else {
            // The input value is String(parsed by PropertiesConfiguration)
            value = this.validateOption(key, value);
        }
        if (this.containsKey(key) && value instanceof List) {
            for (Object item : (List<Object>) value) {
                super.addPropertyDirect(key, item);
            }
        } else {
            super.addPropertyDirect(key, value);
        }
    }

    @Override
    protected void addPropertyInternal(String key, Object value) {
        this.addPropertyDirect(key, value);
    }

    private Object validateOption(String key, Object value) {
        TypedOption<?, ?> option = OptionSpace.get(key);

        if (value instanceof String) {
            return option.parseConvert((String) value);
        }

        Class dataType = option.dataType();
        if (dataType.isInstance(value)) {
            return value;
        }

        throw new IllegalArgumentException(
              String.format("Invalid value for key '%s': '%s'", key, value));
    }

    private void checkRequiredOptions() {
        // TODO: Check required options must be contained in this map
    }

    public void save(File copiedFile) throws ConfigurationException {
        FileHandler fileHandler = new FileHandler(this);
        fileHandler.save(copiedFile);
    }

    @Nullable
    public File getFile() {
        if (StringUtils.isEmpty(this.path)) {
            return null;
        }

        return new File(this.path);
    }

    private static Configuration loadConfigFile(File configFile) {
        E.checkArgument(configFile.exists() &&
                        configFile.isFile() &&
                        configFile.canRead(),
                        "Please specify a proper config file rather than: '%s'",
                        configFile.toString());

        try {
            String fileName = configFile.getName();
            String fileExtension = FilenameUtils.getExtension(fileName);

            Configuration config;
            Configurations configs = new Configurations();

            switch (fileExtension) {
                case "yml":
                case "yaml":
                    Parameters params = new Parameters();
                    FileBasedConfigurationBuilder<FileBasedConfiguration>
                    builder = new FileBasedConfigurationBuilder(
                                  YAMLConfiguration.class)
                                  .configure(params.fileBased()
                                  .setFile(configFile));
                    config = builder.getConfiguration();
                    break;
                case "xml":
                    config = configs.xml(configFile);
                    break;
                default:
                    config = configs.properties(configFile);
                    break;
            }
            return config;
        } catch (ConfigurationException e) {
            throw new ConfigException("Unable to load config: '%s'",
                                      e, configFile);
        }
    }
}
