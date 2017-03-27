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
        return name;
    }

    @Override
    public void index(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public IndexType indexType() {
        return indexType;
    }

    public void indexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public void bindIndex(String propertyKeyName) {
        if (indexMap == null) {
            indexMap = new HashMap<>();
        }
        indexMap.put(indexName, propertyKeyName);
    }

    public boolean containPropertyKey(String name) {
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        return properties.contains(name);
    }

    @Override
    public Set<String> properties() {
        return properties;
    }

    @Override
    public SchemaType properties(String... propertyNames) {
        if (properties == null) {
            properties = new HashSet<>();
        }
        properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    @Override
    public void create() {
        transaction.addVertexLabel(this);
    }

    @Override
    public void remove() {
        transaction.removeVertexLabel(name);
    }

    public Set<String> partitionKey() {
        return partitionKeys;
    }

    public VertexLabel partitionKey(String... keys) {
        if (partitionKeys == null) {
            partitionKeys = new HashSet<>();
        }
        partitionKeys.addAll(Arrays.asList(keys));
        return this;
    }

    public Set<String> clusteringKey() {
        return clusteringKeys;
    }

    public VertexLabel clusteringKey(String... keys) {
        if (clusteringKeys == null) {
            clusteringKeys = new HashSet<>();
        }
        clusteringKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public String toString() {
        return String.format("{name=%s}", name);
    }
}
