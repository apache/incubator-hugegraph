package com.baidu.hugegraph2.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.type.define.IndexType;
import com.baidu.hugegraph2.type.schema.VertexLabel;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeVertexLabel extends VertexLabel {

    private IndexType indexType;

    private Set<String> partitionKeys;
    private Set<String> clusteringKeys;

    private String indexName;
    // key: indexName, val: propertyKeyName
    private Map<String, String> indexMap;

    public HugeVertexLabel(String name, SchemaTransaction transaction) {
        super(name, transaction);
        this.indexType = null;
        this.indexName = null;
        this.indexMap = null;
    }

    public String schema() {
        schema = "schema.vertexLabel(\"" + name + "\")"
                + "." + propertiesSchema()
                + ".create();";
        return schema;
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

    public void create() {
        transaction.addVertexLabel(this);
    }

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
