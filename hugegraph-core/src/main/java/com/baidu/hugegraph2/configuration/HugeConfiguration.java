package com.baidu.hugegraph2.configuration;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph2.HugeGraph;

import java.util.Iterator;

/**
 * Created by liningrui on 17/3/23.
 */
public class HugeConfiguration extends AbstractConfiguration {

    private final PropertiesConfiguration conf;

    public static final Class<? extends Graph> HUGE_GRAPH_CLASS = HugeGraph.class;
    public static final String HUGE_GRAPH_CLASSNAME = HUGE_GRAPH_CLASS.getCanonicalName();

    public static class Keys {
        public static final String GRAPH_CLASS                      = "gremlin.graph";
        public static final String GRAPH_BACKEND                    = "hugegraph.backend";
        public static final String GRAPH_KEYSPACE                   = "hugegraph.keyspace";
        public static final String GRAPH_TABLE_PREFIX               = "hugegraph.tablePrefix";
        public static final String CREATE_TABLES                    = "hugegraph.createTables";
        public static final String STORE_SCHEMA                     = "hugegraph.schema";
        public static final String STORE_INDEX                      = "hugegraph.index";
        public static final String STORE_GRAPH                      = "hugegraph.graph";

    }

    public HugeConfiguration() {
        conf = new PropertiesConfiguration();
        conf.setDelimiterParsingDisabled(true);
        conf.setProperty(Keys.GRAPH_CLASS, HUGE_GRAPH_CLASSNAME);
    }

    public HugeConfiguration(Configuration config) {
        conf = new PropertiesConfiguration();
        conf.setDelimiterParsingDisabled(true);
        conf.setProperty(Keys.GRAPH_CLASS, HUGE_GRAPH_CLASSNAME);
        if (config != null) {
            config.getKeys().forEachRemaining(key -> conf.setProperty(key.replace("..", "."),
                    config.getProperty(key)));
        }
    }

    public String getGraphBackend() {
        return conf.getString(Keys.GRAPH_BACKEND, "memory");
    }

    public HugeConfiguration useMemoryBackend() {
        conf.setProperty(Keys.GRAPH_BACKEND, "memory");
        return this;
    }

    public HugeConfiguration useCassandraBackend() {
        conf.setProperty(Keys.GRAPH_BACKEND, "cassandra");
        return this;
    }


    public String getGraphKeyspace() {
        return conf.getString(Keys.GRAPH_KEYSPACE, "hugegraph");
    }

    public HugeConfiguration setGraphKeyspace(String name) {
        if (!isValidGraphName(name)) {
            throw new IllegalArgumentException("Invalid graph keyspace."
                    + " Only alphanumerics and underscores are allowed");
        }

        conf.setProperty(Keys.GRAPH_KEYSPACE, name);
        return this;
    }

    public String getGraphTablePrefix() {
        return conf.getString(Keys.GRAPH_TABLE_PREFIX, "");
    }

    public HugeConfiguration setGraphTablePrefix(String name) {
        if (!isValidGraphName(name)) {
            throw new IllegalArgumentException("Invalid graph table prefix."
                    + " Only alphanumerics and underscores are allowed");
        }

        conf.setProperty(Keys.GRAPH_TABLE_PREFIX, name);
        return this;
    }

    private static boolean isValidGraphName(String name) {
        return name.matches("^[A-Za-z0-9_]+$");
    }

    public boolean getCreateTables() {
        return conf.getBoolean(Keys.CREATE_TABLES, false);
    }

    public HugeConfiguration setCreateTables(boolean create) {
        conf.setProperty(Keys.CREATE_TABLES, create);
        return this;
    }

    public String getStoreSchema() {
        return conf.getString(Keys.STORE_SCHEMA, "huge_schema");
    }

    public HugeConfiguration setStoreSchema(String name) {
        if (!isValidGraphName(name)) {
            throw new IllegalArgumentException("Invalid graph table prefix."
                    + " Only alphanumerics and underscores are allowed");
        }

        conf.setProperty(Keys.STORE_SCHEMA, name);
        return this;
    }

    public String getStoreIndex() {
        return conf.getString(Keys.STORE_INDEX, "huge_index");
    }

    public HugeConfiguration setStoreIndex(String name) {
        if (!isValidGraphName(name)) {
            throw new IllegalArgumentException("Invalid graph table prefix."
                    + " Only alphanumerics and underscores are allowed");
        }

        conf.setProperty(Keys.STORE_INDEX, name);
        return this;
    }

    public String getStoreGraph() {
        return conf.getString(Keys.STORE_GRAPH, "huge_graph");
    }

    public HugeConfiguration setStoreGraph(String name) {
        if (!isValidGraphName(name)) {
            throw new IllegalArgumentException("Invalid graph table prefix."
                    + " Only alphanumerics and underscores are allowed");
        }

        conf.setProperty(Keys.STORE_GRAPH, name);
        return this;
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

    public HugeConfiguration set(String key, Object value) {
        conf.setProperty(key, value);
        return this;
    }

    @Override
    public Iterator<String> getKeys() {
        return conf.getKeys();
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        conf.setProperty(key, value);
    }
}
