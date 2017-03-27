/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */

package com.baidu.hugegraph2.configuration;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Iterator;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.HugeException;
import com.google.common.base.Preconditions;

public class HugeConfiguration extends AbstractConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(HugeConfiguration.class);

    // TODO:最好定义一个类继承PropertiesConfiguration，并且加上所有ConfigOption支持的类的get方法
    private PropertiesConfiguration configuration;


    public HugeConfiguration(String configurationFile) {
        File file = new File(configurationFile);
        Preconditions.checkArgument(file != null && file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable configuration file, but was given: %s", file.toString());
        try {
            configuration = new PropertiesConfiguration(file);
        } catch (ConfigurationException e) {
            logger.error(e.getMessage());
            throw new HugeException(e.getMessage());
        }
    }


    public HugeConfiguration(Configuration config) {
        if (configuration == null) {
            configuration = new PropertiesConfiguration();
            configuration.setDelimiterParsingDisabled(true);
        }

        if (config != null) {
            config.getKeys().forEachRemaining(key ->
                    configuration.setProperty(key.replace("..", "."), config.getProperty(key)));
        }
        updateDefaultConfiguration();
    }


    public void updateDefaultConfiguration() {
        try {
            Iterator<String> keys = configuration.getKeys();
            while (keys.hasNext()) {
                String key = keys.next();
                // 如果该key已经注册过，表示可用
                if (ConfigSpace.containKey(key)) {
                    ConfigOption configOption = ConfigSpace.get(key);

                    // 获取到option的数据类型
                    Class dataType = configOption.dataType();
                    String getMethod = "get" + dataType.getSimpleName();
                    // 得到对应的get方法
                    Method method = configuration.getClass().getMethod(getMethod, String.class, dataType);
                    configOption.value(method.invoke(configuration, key, configOption.value()));
                } else {
                    logger.warn("设置了冗余的配置项：" + key);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new HugeException(e.getMessage());
        }
    }


    /**
     * @param option
     * @param <T>
     * @return
     */
    public<T> T get(ConfigOption<T> option) {
        return option.value();
    }

    @Override
    public boolean isEmpty() {
        return configuration.isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return configuration.containsKey(key);
    }

    @Override
    public Object getProperty(String key) {
        return configuration.getProperty(key);
    }

    public HugeConfiguration set(String key, Object value) {
        configuration.setProperty(key, value);
        return this;
    }

    @Override
    public Iterator<String> getKeys() {
        return configuration.getKeys();
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        configuration.setProperty(key, value);
    }

}
