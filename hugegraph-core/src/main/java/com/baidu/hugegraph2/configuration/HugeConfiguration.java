package com.baidu.hugegraph2.configuration;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Iterator;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeConfiguration extends AbstractConfiguration {

    private final PropertiesConfiguration conf;

    public HugeConfiguration() {
        conf = new PropertiesConfiguration();
        conf.setDelimiterParsingDisabled(true);
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        conf.addProperty(key, value);
    }

    @Override
    public boolean isEmpty() {
        return conf.isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return conf.containsKey(key);
    }

    @Override
    public Object getProperty(String key) {
        return conf.getProperty(key);
    }

    @Override
    public Iterator<String> getKeys() {
        return conf.getKeys();
    }
}
