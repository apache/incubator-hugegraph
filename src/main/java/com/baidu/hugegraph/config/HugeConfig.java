/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
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
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.exception.ConfigException;
import com.baidu.hugegraph.util.E;

public class HugeConfig extends PropertiesConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(HugeConfig.class);

    public HugeConfig(Configuration config) {
        if (config == null) {
            throw new ConfigException("Passed config object is null");
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
        E.checkArgument(!fileName.isEmpty(), "The config file can't be empty");

        File file = new File(fileName);
        E.checkArgument(file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable config file, " +
                "but got: %s", file.toString());
        return file;
    }

    public void updateDefaultOption() {
        try {
            Iterator<String> keys = this.getKeys();
            while (keys.hasNext()) {
                String key = keys.next();
                if (!OptionSpace.containKey(key)) {
                    logger.warn("The option: '%s' is redundant", key);
                    continue;
                }
                ConfigOption option = OptionSpace.get(key);
                Class dataType = option.dataType();
                String getMethod = "get" + dataType.getSimpleName();
                Method method = this.getClass()
                        .getMethod(getMethod, String.class, dataType);
                option.value(method.invoke(this, key, option.value()));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConfigException(e.getMessage());
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
