/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.Iterator;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Created by zhangsuochao on 17/2/7.
 */
public class HugeGraphConfiguration extends AbstractConfiguration {

    private final PropertiesConfiguration conf;

    public HugeGraphConfiguration() {
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

    public String getGraphNamespace() {
        return conf.getString(Keys.GRAPH_NAMESPACE, "default");
    }

    public static class Keys {
        public static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
        public static final String ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
        public static final String GRAPH_NAMESPACE = "gremlin.hbase.namespace";
    }
}
