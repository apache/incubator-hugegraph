package com.baidu.hugegraph2.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph2.IndexType;
import com.baidu.hugegraph2.type.schema.VertexLabel;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeVertexLabel implements VertexLabel {

    private String name;
    private IndexType indexType;

    private Set<String> properties;
    private Set<String> partitionKeys;
    private Set<String> clusteringKeys;


    private String indexName;
    // key: indexName, val: propertyKeyName
    private Map<String, String> indexMap;


    public HugeVertexLabel(String name) {
        this.name = name;
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

    public void properties(String... propertyNames) {
        if (properties == null) {
            properties = new HashSet<>();
        }
        properties.addAll(Arrays.asList(propertyNames));
    }

    public Set<String> partitionKeys() {
        return partitionKeys;
    }

    public void partitionKeys(String... keys) {
        if (partitionKeys == null) {
            partitionKeys = new HashSet<>();
        }
        partitionKeys.addAll(Arrays.asList(keys));
    }

    public Set<String> clusteringKeys() {
        return clusteringKeys;
    }

    public void clusteringKey(String... keys) {
        if (clusteringKeys == null) {
            clusteringKeys = new HashSet<>();
        }
        clusteringKeys.addAll(Arrays.asList(keys));
    }

    @Override
    public String toString() {
        return String.format("{name=%s}", name);
    }
}
