package com.baidu.hugegraph2.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.type.define.IndexType;
import com.baidu.hugegraph2.type.schema.SchemaType;
import com.baidu.hugegraph2.type.schema.VertexLabel;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeVertexLabel implements VertexLabel {

    private String name;
    private SchemaTransaction transaction;
    private IndexType indexType;

    private Set<String> properties;
    private Set<String> partitionKeys;
    private Set<String> clusteringKeys;

    private String indexName;
    // key: indexName, val: propertyKeyName
    private Map<String, String> indexMap;

    public HugeVertexLabel(String name, SchemaTransaction transaction) {
        this.name = name;
        this.transaction = transaction;
        this.indexType = null;
        this.properties = null;
        this.indexName = null;
        this.indexMap = null;
    }

    @Override
    public String schema() {
        return null;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void index(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public IndexType indexType() {
        return this.indexType;
    }

    public void indexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public void bindIndex(String propertyKeyName) {
        if (this.indexMap == null) {
            this.indexMap = new HashMap<>();
        }
        this.indexMap.put(this.indexName, propertyKeyName);
    }

    public boolean containPropertyKey(String name) {
        if (this.properties == null || this.properties.isEmpty()) {
            return false;
        }
        return this.properties.contains(name);
    }

    @Override
    public Set<String> properties() {
        return this.properties;
    }

    @Override
    public SchemaType properties(String... propertyNames) {
        if (this.properties == null) {
            this.properties = new HashSet<>();
        }
        this.properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    @Override
    public void create() {
        this.transaction.addVertexLabel(this);
    }

    @Override
    public void remove() {
        this.transaction.removeVertexLabel(this.name);
    }

    public Set<String> partitionKey() {
        return this.partitionKeys;
    }

    @Override
    public VertexLabel partitionKey(String... keys) {
        if (this.partitionKeys == null) {
            this.partitionKeys = new HashSet<>();
        }
        this.partitionKeys.addAll(Arrays.asList(keys));
        return this;
    }

    public Set<String> clusteringKey() {
        return this.clusteringKeys;
    }

    @Override
    public VertexLabel clusteringKey(String... keys) {
        if (this.clusteringKeys == null) {
            this.clusteringKeys = new HashSet<>();
        }
        this.clusteringKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public Set<String> sortKeys() {
        // TODO: implement
        Set<String> s = new HashSet<>();
        s.add("name");
        return s;
    }

    @Override
    public String toString() {
        return String.format("{name=%s}", this.name);
    }
}
